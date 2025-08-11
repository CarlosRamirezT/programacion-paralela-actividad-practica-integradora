import pykka

class ValidatorActor(pykka.ThreadingActor):
    def __init__(self, numbers):
        super().__init__()
        self.numbers = numbers

    def on_receive(self, message):
        if message.get("cmd") == "validate":
            if not isinstance(self.numbers, list) or not self.numbers:
                return {"ok": False, "error": "numbers must be a non-empty list"}
            try:
                _ = [float(x) for x in self.numbers]
            except Exception:
                return {"ok": False, "error": "numbers must be numeric"}
            return {"ok": True}
        return {"ok": False, "error": "unknown command"}
