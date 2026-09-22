"""Tiny in-memory Fastly Object Storage seam shared by both test runners."""

from io import BytesIO


class TinyFos:
    def __init__(self):
        self.objects = {}
        self.puts = 0

    def put_object(self, *, Bucket, Key, Body, Metadata, **kwargs):
        self.objects[Key] = (Body, Metadata)
        self.puts += 1

    def head_object(self, *, Bucket, Key):
        payload, metadata = self.objects[Key]
        return {"ContentLength": len(payload), "Metadata": metadata}

    def get_object(self, *, Bucket, Key):
        payload, metadata = self.objects[Key]
        return {"ContentLength": len(payload), "Metadata": metadata, "Body": BytesIO(payload)}
