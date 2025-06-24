class City:
    def __init__(self, name):
        self.name = name
        self.latitude = None
        self.longitude = None

    def set_coordinates(self, lat, lon):
        self.latitude = lat
        self.longitude = lon

    def to_dict(self):
        return {"name": self.name, "lat": self.latitude, "lon": self.longitude}
