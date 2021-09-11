from pi_mqtt_gpio.modules import GenericGPIO


REQUIREMENTS = ("pifaceio", "pifacecommon")

DIRECTIONS = None
PULLUPS = None


class GPIO(GenericGPIO):
    """
    Implementation of GPIO class for PiFaceDigital IO board.
    """

    def __init__(self, config):
        import pifaceio as pfdio

        pfdio.init()
        self.io = pfdio

    def setup_pin(self, pin, direction, pullup, pin_config):
        pass

    def set_pin(self, pin, value):
        self.io.digital_write(pin, value)

    def get_pin(self, pin):
        return self.io.digital_read(pin)

    def cleanup(self):
        self.io.deinit()
