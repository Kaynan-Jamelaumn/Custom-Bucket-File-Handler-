import handler
from secrets import access_key, secret_access_key

class Event:
    events = ('write', 'overwrite', 'read', 'download_all_files', 'erase')

    def __init__(self):
        self.__file = None
        self.__event = None
        self.__content = None
        self.__prefix = None
        self.__bucket = []
        self.__path = None
        self.__jsonfy = False
        self.__directory = None
        self.__exception = None
        self.__only_extension = None
        self.__dir_erase = False
        self.__extension = None

    @property
    def only_extension(self):
        return self.__only_extension

    @only_extension.setter
    def only_extension(self, value: str | list | tuple):
        self.__only_extension = value

    @property
    def file(self):
        return self.__file

    @file.setter
    def file(self, value: str | dict):
        self.__file = value

    @property
    def event(self):
        return self.__event

    @event.setter
    def event(self, value: str):
        if value.lower() in Event.events:
            self.__event = value.lower()

    @property
    def content(self):
        return self.__content

    @content.setter
    def content(self, value: str):
        self.__content = value

    @property
    def prefix(self):
        return self.__prefix

    @prefix.setter
    def prefix(self, value: str):
        self.__prefix = value

    @property
    def path(self):
        return self.__path

    @path.setter
    def path(self, value: str):
        self.__path = value

    @property
    def jsonfy(self):
        return self.__jsonfy

    @jsonfy.setter
    def jsonfy(self, value: bool):
        # fictional value
        if type(value) == bool:
            self.__jsonfy = value

    @property
    def bucket(self):
        return self.__bucket

    @bucket.setter
    def bucket(self, value: list | tuple):
        self.__bucket = value

    @property
    def directory(self):
        return self.__directory

    @directory.setter
    def directory(self, value: str):
        self.__directory = value

    @property
    def extension(self):
        return self.__extension

    @extension.setter
    def extension(self, value: str):
        self.__extension = value

    @property
    def dir_erase(self):
        return self.__dir_erase

    @dir_erase.setter
    def dir_erase(self, value: bool):
        # fictional value
        if type(value) == bool:
            self.__dir_erase = value

    @property
    def exception(self):
        return self.__exception

    @exception.setter
    def exception(self, value: str | list | tuple):
        self.__exception = value


object = []
a = Event()
a.event = 'download_all_files'
a.bucket = [access_key, secret_access_key, ' your bucket-name']
object.append(a)
handler.events_to_do(object)
#print(handler.events_to_do(object))
