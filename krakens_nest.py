class Event(list):
    def __call__(self, *args, **kwargs):
        for item in self:
            item(*args, **kwargs)

class Kraken:
    def __init__(self):
        self.events = Event()

    def move_tentacle(self, args):
        self.events(args)

