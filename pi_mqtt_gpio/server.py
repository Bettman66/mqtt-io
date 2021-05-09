import argparse
import json
import logging
import logging.config
import socket
import ssl
import sys
import threading
from hashlib import sha1
from importlib import import_module
from time import sleep, time

import cerberus
import paho.mqtt.client as mqtt
import yaml

from pi_mqtt_gpio import CONFIG_SCHEMA
from pi_mqtt_gpio.modules import BASE_SCHEMA, InterruptEdge, PinDirection, PinPullup
from pi_mqtt_gpio.scheduler import Scheduler, Task

LOG_LEVEL_MAP = {
    mqtt.MQTT_LOG_INFO: logging.INFO,
    mqtt.MQTT_LOG_NOTICE: logging.INFO,
    mqtt.MQTT_LOG_WARNING: logging.WARNING,
    mqtt.MQTT_LOG_ERR: logging.ERROR,
    mqtt.MQTT_LOG_DEBUG: logging.DEBUG,
}

RECONNECT_DELAY_SECS = 5
GPIO_MODULES = {}  # storage for gpio modules
GPIO_CONFIGS = {}  # storage for gpios
LAST_STATES = {}
GPIO_INTERRUPT_LOOKUP = {}
OUTPUT_TOPIC = "output"
INPUT_TOPIC = "input"

_LOG = logging.getLogger("mqtt_gpio")


class CannotInstallModuleRequirements(Exception):
    pass


class InvalidPayload(Exception):
    pass


class ModuleConfigInvalid(Exception):
    def __init__(self, errors, *args, **kwargs):
        self.errors = errors
        super(ModuleConfigInvalid, self).__init__(*args, **kwargs)


class ConfigValidator(cerberus.Validator):
    """
    Cerberus Validator containing function(s) for use with validating or
    coercing values relevant to the pi_mqtt_gpio project.
    """

    @staticmethod
    def _normalize_coerce_rstrip_slash(value):
        """
        Strip forward slashes from the end of the string.
        :param value: String to strip forward slashes from
        :type value: str
        :return: String without forward slashes on the end
        :rtype: str
        """
        return value.rstrip("/")

    @staticmethod
    def _normalize_coerce_tostring(value):
        """
        Convert value to string.
        :param value: Value to convert
        :return: Value represented as a string.
        :rtype: str
        """
        return str(value)

def on_log(client, userdata, level, buf):
    """
    Called when MQTT client wishes to log something.
    :param client: MQTT client instance
    :param userdata: Any user data set in the client
    :param level: MQTT log level
    :param buf: The log message buffer
    :return: None
    :rtype: NoneType
    """
    _LOG.log(LOG_LEVEL_MAP[level], "MQTT client: %s" % buf)


def set_pin(topic_prefix, output_config, value):
    """
    Sets the output pin to a new value and publishes it on MQTT.
    :param topic_prefix: the name of the topic, the pin is published
    :type topic_prefix: string
    :param output_config: The output configuration
    :type output_config: dict
    :param value: The new value to set it to
    :type value: bool
    :return: None
    :rtype: NoneType
    """
    gpio = GPIO_MODULES[output_config["module"]]
    set_value = not value if output_config["inverted"] else value
    gpio.set_pin(output_config["pin"], set_value)
    _LOG.info(
        "Set %r output %r to %r",
        output_config["module"],
        output_config["name"] ,
        set_value,
    )

def get_pin(in_conf, module):
    """
    Gets a pin using a GPIO module. Inverts the state if set in config."
    :param in_conf: The input config
    :type in_conf: dict
    :param module: The GPIO module
    :type module: modules.GenericGPIO
    :return: The value of the pin, inverted if desired
    :rtype: bool
    """
    state = bool(module.get_pin(in_conf["pin"]))
    return state != in_conf["inverted"]

def output_by_name(output_name):
    """
    Returns the output configuration for a given output name.
    :param output_name: The name of the output
    :type output_name: str
    :return: The output configuration or None if not found
    :rtype: dict
    """
    for output in digital_outputs:
        if output["name"] == output_name:
            return output
    _LOG.warning("No output found with name of %r", output_name)

def handle_set(topic_prefix, msg):
    """
    Handles an incoming 'set' MQTT message.
    :param topic_prefix: the name of the topic, the pin is published
    :type topic_prefix: string
    :param msg: The incoming MQTT message
    :type msg: paho.mqtt.client.MQTTMessage
    :return: None
    :rtype: NoneType
    """
    output_name = output_name_from_topic(msg.topic, topic_prefix)
    output_config = output_by_name(output_name)
    if output_config is None:
        return
    payload = msg.payload.decode("utf8")
    if payload not in (output_config["on_payload"], output_config["off_payload"]):
        _LOG.warning(
            "Payload %r does not relate to configured on/off values %r and %r",
            payload,
            output_config["on_payload"],
            output_config["off_payload"],
        )
        return

    value = payload == output_config["on_payload"]
    set_pin(topic_prefix, output_config, value)

def install_missing_requirements(module):
    """
    Some of the modules require external packages to be installed. This gets
    the list from the `REQUIREMENTS` module attribute and attempts to
    install the requirements using pip.
    :param module: GPIO module
    :type module: ModuleType
    :return: None
    :rtype: NoneType
    """
    reqs = getattr(module, "REQUIREMENTS", [])
    if not reqs:
        _LOG.info("Module %r has no extra requirements to install.", module)
        return
    import pkg_resources

    pkgs_installed = pkg_resources.WorkingSet()
    pkgs_required = []
    for req in reqs:
        if pkgs_installed.find(pkg_resources.Requirement.parse(req)) is None:
            pkgs_required.append(req)
    if pkgs_required:
        from subprocess import CalledProcessError, check_call

        try:
            check_call([sys.executable, "-m", "pip", "install"] + pkgs_required)
        except CalledProcessError as err:
            raise CannotInstallModuleRequirements(
                "Unable to install packages for module %r (%s): %s"
                % (module, pkgs_required, err)
            )


def type_from_topic(topic, topic_prefix):
    """
    Return the topic type for this topic
    The topics are formatted as topic_prefix/type[/parameter...].
    The parameter section is optional.
    :param topic: String such as 'mytopicprefix/output/tv_lamp/set'
    :type topic: str
    :param topic_prefix: Prefix of our topicsclient,
    :type topic_prefix: str
    :return: Type for this topic
    :rtype: str
    """
    # Strip off the prefix
    lindex = len("%s/" % topic_prefix)
    s = topic[lindex:]
    # Get remaining fields, the first one is the type
    fields = s.split("/")
    return fields[0]

def output_name_from_topic(topic, topic_prefix):
    """
    Return the name of the output which the topic is setting.
    :param topic: String such as 'mytopicprefix/output/tv_lamp'
    :type topic: str
    :param topic_prefix: Prefix of our topicsclient,
    :type topic_prefix: str
    :return: Name of the output this topic is setting
    :rtype: str
    """

    lindex = len("%s/%s/" % (topic_prefix, OUTPUT_TOPIC))
    rindex = len(topic)
    return topic[lindex:rindex]

def init_mqtt(config, digital_outputs):
    """
    Configure MQTT client.
    :param config: Validated config dict containing MQTT connection details
    :type config: dict
    :param digital_outputs: List of validated config dicts for digital outputs
    :type digital_outputs: list
    :return: Connected and initialised MQTT client
    :rtype: paho.mqtt.client.Client
    """
    global topic_prefix
    topic_prefix = config["topic_prefix"]
    protocol = mqtt.MQTTv311
    if config["protocol"] == "3.1":
        protocol = mqtt.MQTTv31

    # https://stackoverflow.com/questions/45774538/what-is-the-maximum-length-of-client-id-in-mqtt
    # TLDR: Soft limit of 23, but we needn't truncate it on our end.
    client_id = config["client_id"]
    if not client_id:
        client_id = "pi-mqtt-gpio-%s" % sha1(topic_prefix.encode("utf8")).hexdigest()

    client = mqtt.Client(client_id=client_id, clean_session=False, protocol=protocol)

    if config["user"] and config["password"]:
        client.username_pw_set(config["user"], config["password"])

    # Set last will and testament (LWT)
    status_topic = "%s/%s" % (topic_prefix, config["status_topic"])
    client.will_set(
        status_topic, payload=config["status_payload_dead"], qos=1, retain=True
    )
    _LOG.debug("Last will set on %r as %r.", status_topic, config["status_payload_dead"])

    # Set TLS options
    tls_enabled = config.get("tls", {}).get("enabled")
    if tls_enabled:
        tls_config = config["tls"]
        tls_kwargs = dict(
            ca_certs=tls_config.get("ca_certs"),
            certfile=tls_config.get("certfile"),
            keyfile=tls_config.get("keyfile"),
            ciphers=tls_config.get("ciphers"),
        )
        try:
            tls_kwargs["cert_reqs"] = getattr(ssl, tls_config["cert_reqs"])
        except KeyError:
            pass
        try:
            tls_kwargs["tls_version"] = getattr(ssl, tls_config["tls_version"])
        except KeyError:
            pass

        client.tls_set(**tls_kwargs)
        client.tls_insecure_set(tls_config["insecure"])

    def on_conn(client, userdata, flags, rc):
        """
        On connection to MQTT, subscribe to the relevant topics.
        :param client: Connected MQTT client instance
        :type client: paho.mqtt.client.Client
        :param userdata: User data
        :param flags: Response flags from the broker
        :type flags: dict
        :param rc: Response code from the broker
        :type rc: int
        :return: None
        :rtype: NoneType
        """
        if rc == 0:
            _LOG.info(
                "Connected to the MQTT broker with protocol v%s.", config["protocol"]
            )
            for out_conf in digital_outputs:
                topic = "%s/%s/%s" % (
                    topic_prefix,
                    OUTPUT_TOPIC,
                    out_conf["name"],
                )
                client.subscribe(topic, qos=1)
                _LOG.info("Subscribed to topic: %r", topic)
            client.publish(
                status_topic, config["status_payload_running"], qos=1, retain=True
            )
        elif rc == 1:
            _LOG.fatal("Incorrect protocol version used to connect to MQTT broker.")
            sys.exit(1)
        elif rc == 2:
            _LOG.fatal("Invalid client identifier used to connect to MQTT broker.")
            sys.exit(1)
        elif rc == 3:
            _LOG.warning("MQTT broker unavailable. Retrying in %s secs...")
            sleep(RECONNECT_DELAY_SECS)
            client.reconnect()
        elif rc == 4:
            _LOG.fatal("Bad username or password used to connect to MQTT broker.")
            sys.exit(1)
        elif rc == 5:
            _LOG.fatal("Not authorised to connect to MQTT broker.")
            sys.exit(1)

    def on_msg(client, userdata, msg):
        """
        On reception of MQTT message, set the relevant output to a new value.
        :param client: Connected MQTT client instance
        :type client: paho.mqtt.client.Client
        :param userdata: User data (any data type)
        :param msg: Received message instance
        :type msg: paho.mqtt.client.MQTTMessage
        :return: None
        :rtype: NoneType
        """
        try:
            topic_type = type_from_topic(msg.topic, topic_prefix)
            _LOG.info(
                "Received message on topic %r type: %r: %r",
                msg.topic,
                topic_type,
                msg.payload,
            )
            if topic_type == OUTPUT_TOPIC:
                handle_set(topic_prefix, msg)
            else:
                _LOG.warning("Unhandled topic %r.", msg.topic)
        except InvalidPayload as exc:
            _LOG.warning("Invalid payload on received MQTT message: %s" % exc)
        except Exception:
            _LOG.exception("Exception while handling received MQTT message:")

    client.on_connect = on_conn
    client.on_message = on_msg
    client.on_log = on_log

    return client


def configure_gpio_module(gpio_config):
    """
    Imports gpio module, validates its config and returns an instance of it.
    :param gpio_config: Module configuration values
    :type gpio_config: dict
    :return: Configured instance of the gpio module
    :rtype: pi_mqtt_gpio.modules.GenericGPIO
    """
    gpio_module = import_module("pi_mqtt_gpio.modules.%s" % gpio_config["module"])
    # Doesn't need to be a deep copy because we won't modify the base
    # validation rules, just add more of them.
    module_config_schema = BASE_SCHEMA.copy()
    module_config_schema.update(getattr(gpio_module, "CONFIG_SCHEMA", {}))
    module_validator = cerberus.Validator(module_config_schema)
    if not module_validator.validate(gpio_config):
        raise ModuleConfigInvalid(module_validator.errors)
    gpio_config = module_validator.normalized(gpio_config)
    install_missing_requirements(gpio_module)
    return gpio_module.GPIO(gpio_config)


def initialise_digital_input(in_conf, gpio):
    """
    Initialises digital input.
    :param in_conf: Input config
    :type in_conf: dict
    :param gpio: Instance of GenericGPIO to use to configure the pin
    :type gpio: pi_mqtt_gpio.modules.GenericGPIO
    :return: None
    :rtype: NoneType
    """
    pin = in_conf["pin"]
    pud = None
    if in_conf["pullup"]:
        pud = PinPullup.UP
    elif in_conf["pulldown"]:
        pud = PinPullup.DOWN

    gpio.setup_pin(pin, PinDirection.INPUT, pud, in_conf)

    # try to initialize interrupt, if available
    if in_conf["interrupt"] != "none":
        try:
            edge = {
                "rising": InterruptEdge.RISING,
                "falling": InterruptEdge.FALLING,
                "both": InterruptEdge.BOTH,
            }[in_conf["interrupt"]]
        except KeyError as exc:
            _LOG.error(
                "initialise_digital_input: config value(%s) for 'interrupt' \
                 invalid in  entry '%s'",
                in_conf["interrupt"],
                in_conf["name"],
            )

        try:
            bouncetime = in_conf["bouncetime"]
            module = in_conf["module"]
            gpio.setup_interrupt(module, pin, edge, gpio_interrupt_callback, bouncetime)

            # store for callback function handling
            if not GPIO_INTERRUPT_LOOKUP.get(module):
                GPIO_INTERRUPT_LOOKUP[module] = {}
            if not GPIO_INTERRUPT_LOOKUP[module].get(pin):
                GPIO_INTERRUPT_LOOKUP[module][pin] = in_conf
        except NotImplementedError as exc:
            _LOG.error(
                "initialise_digital_input: interrupt not implemented for \
                 input(%s) on module(%s): %s",
                in_conf["name"],
                in_conf["module"],
                exc,
            )


def initialise_digital_output(out_conf, gpio):
    """
    Initialises digital output.
    :param out_conf: Output config
    :type out_conf: dict
    :param gpio: Instance of GenericGPIO to use to configure the pin
    :type gpio: pi_mqtt_gpio.modules.GenericGPIO
    :return: None
    :rtype: NoneType
    """
    gpio.setup_pin(out_conf["pin"], PinDirection.OUTPUT, None, out_conf)


def gpio_interrupt_callback(module, pin):
    try:
        in_conf = GPIO_INTERRUPT_LOOKUP[module][pin]
    except KeyError as exc:
        _LOG.error(
            "gpio_interrupt_callback: no interrupt configured \
            for pin '%s' on module %s: %s",
            pin,
            module,
            exc,
        )
    _LOG.info("Interrupt: Input %r triggered", in_conf["name"])

    # publish the interrupt trigger
    client.publish(
        "%s/%s/%s" % (topic_prefix, INPUT_TOPIC, in_conf["name"]),
        payload=in_conf["interrupt_payload"],
        retain=in_conf["retain"],
    )


def main(args):
    global digital_inputs
    global digital_outputs
    global client
    global scheduler

    _LOG.info("Startup")

    with open(args.config) as f:
        config = yaml.safe_load(f)
    validator = ConfigValidator(CONFIG_SCHEMA)
    if not validator.validate(config):
        _LOG.error("Config did not validate:\n%s", yaml.dump(validator.errors))
        sys.exit(1)
    config = validator.normalized(config)

    # Remove all handlers associated with the root logger object.
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    # and load the new config from the config file
    logging.config.dictConfig(config["logging"])

    digital_inputs = config["digital_inputs"]
    digital_outputs = config["digital_outputs"]

    client = init_mqtt(config["mqtt"], config["digital_outputs"])
    topic_prefix = config["mqtt"]["topic_prefix"]

    # Install modules for GPIOs
    for gpio_config in config["gpio_modules"]:
        GPIO_CONFIGS[gpio_config["name"]] = gpio_config
        try:
            GPIO_MODULES[gpio_config["name"]] = configure_gpio_module(gpio_config)
        except ModuleConfigInvalid as exc:
            _LOG.error(
                "Config for %r module named %r did not validate:\n%s",
                gpio_config["module"],
                gpio_config["name"],
                yaml.dump(exc.errors),
            )
            sys.exit(1)

    for in_conf in digital_inputs:
        initialise_digital_input(in_conf, GPIO_MODULES[in_conf["module"]])
        LAST_STATES[in_conf["name"]] = None

    for out_conf in digital_outputs:
        initialise_digital_output(out_conf, GPIO_MODULES[out_conf["module"]])

    try:
        client.connect(
            config["mqtt"]["host"], config["mqtt"]["port"], config["mqtt"]["keepalive"]
        )
    except socket.error as err:
        _LOG.fatal("Unable to connect to MQTT server: %s" % err)
        sys.exit(1)
    client.loop_start()

    for out_conf in digital_outputs:
        # If configured to do so, publish the initial states of the outputs
        initial_setting = out_conf.get("initial")
        if initial_setting is not None and out_conf.get("publish_initial", False):
            payload = out_conf[
                "on_payload" if initial_setting == "high" else "off_payload"
            ]
            client.publish(
                "%s/%s/%s" % (topic_prefix, OUTPUT_TOPIC, out_conf["name"]),
                retain=out_conf["retain"],
                payload=payload,
            )

    scheduler = Scheduler()

    try:
        while True:
            for in_conf in digital_inputs:
                # Only read pins that are not configured as interrupt.
                # Read interrupts once at startup (startup_read)
                if in_conf["interrupt"] != "none":
                    continue
                gpio = GPIO_MODULES[in_conf["module"]]
                state = get_pin(in_conf, gpio)
                sleep(0.01)
                if get_pin(in_conf, gpio) != state:
                    continue
                if state != LAST_STATES[in_conf["name"]]:
                    _LOG.info(
                        "Polling: Input %r state changed to %r", in_conf["name"], state
                    )
                    client.publish(
                        "%s/%s/%s" % (topic_prefix, INPUT_TOPIC, in_conf["name"]),
                        payload=(
                            in_conf["on_payload"] if state else in_conf["off_payload"]
                        ),
                        retain=in_conf["retain"],
                    )
                    LAST_STATES[in_conf["name"]] = state
            scheduler.loop()
            sleep(0.01)
    except KeyboardInterrupt:
        print("")
    finally:
        client.publish(
            "%s/%s" % (topic_prefix, config["mqtt"]["status_topic"]),
            config["mqtt"]["status_payload_stopped"],
            qos=1,
            retain=True,
        )

        client.loop_stop()
        client.disconnect()
        client.loop_forever()

        for name, gpio in GPIO_MODULES.items():
            if not GPIO_CONFIGS[name]["cleanup"]:
                _LOG.info("Cleanup disabled for module %r.", name)
                continue
            try:
                gpio.cleanup()
            except Exception:
                _LOG.exception("Unable to execute cleanup routine for module %r:", name)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG, format="%(asctime)s %(name)s (%(levelname)s): %(message)s"
    )

    p = argparse.ArgumentParser()
    p.add_argument("config")
    args = p.parse_args()
    main(args)
