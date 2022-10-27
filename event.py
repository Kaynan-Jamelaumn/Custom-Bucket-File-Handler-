import handler
from secrets import access_key, secret_access_key
#from project_1.code import overwrite


class Event:
  events = ('write', 'overwrite', 'read', 'download_all_files', 'erase')

  def __init__(self):
    self.__file = ''
    self.__event = ''
    self.__content = ''
    self.__prefix = ''
    self.__bucket = []
    self.__path = ''
    self.__jsonfy = False
    self.__directory = ''
    self.__dir_erase = False
    self.__extension = ''

  @property
  def file(self):
    return self.__file

  @file.setter
  def file(self, value):
    self.__file = value

  @property
  def event(self):
    return self.__event

  @event.setter
  def event(self, value):
    if value.lower() in Event.events:
      self.__event = value.lower()

  @property
  def content(self):
    return self.__content

  @content.setter
  def content(self, value):
    self.__content = value

  @property
  def prefix(self):
    return self.__prefix

  @prefix.setter
  def prefix(self, value):
    self.__prefix = value

  @property
  def path(self):
    return self.__path

  @path.setter
  def path(self, value):
    self.__path = value

  @property
  def jsonfy(self):
    return self.__jsonfy

  @jsonfy.setter
  def jsonfy(self):
    if self.jsonfy != True:
      self.__jsonfy = True
    else:
      self.__jsonfy = False

  @property
  def bucket(self):
    return self.__bucket

  @bucket.setter
  def bucket(self, value):
    self.__bucket = value

  @property
  def directory(self):
    return self.__directory

  @directory.setter
  def directory(self, value):
    self.__directory = value

  @property
  def extension(self):
    return self.__extension

  @extension.setter
  def extension(self, value):
    self.__extension = value

  @property
  def dir_erase(self):
    return self.__dir_erase

  @dir_erase.setter
  def dir_erase(self):
    if self.__dir_erase != True:

      self.__dir_erase = True
    else:
      self.__dir_erase = False


object = []
a = Event()
a.event = 'download_all_files'
a.bucket = [access_key, secret_access_key, ' your bucket-name']
a.prefix = 'Video'
object.append(a)

print(handler.events_to_do(object))
