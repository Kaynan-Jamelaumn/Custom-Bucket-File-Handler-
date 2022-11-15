import code
from secrets import access_key, secret_access_key
# from project_1.code import overwrite
import os
import json
import boto3


class Event:
    events = ('write', 'overwrite', 'read', 'download_all_files', 'erase')
    format_type = [
        'png', 'jpg', 'tiff', 'gif', 'eps', 'svg', 'jpeg', 'bmp', 'psd', 'raw',
        'webp', 'avi', 'mp4', 'm4v', 'mov', 'mpg', 'mpeg', 'wmv', 'aiff', 'au',
        'mid', 'midi', 'mp3', 'm4a', 'wav', 'wma'
    ]

    def __init__(self):
        self.__file = None
        self.__event = None
        self.__content = None
        self.__prefix = None
        self.__bucket = []
        self.__path = None
        self.__jsonfy = False
        self.__directory = 'bucket_files'
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
    def file(self, value: str):
        if value[0] == '\\':
            value = value[1:]
        if value[-1] == '\\':
            value = value[:-1]
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
        if self.__file != None:
            if len(self.__path.split('.')) > 1:
                pass
            else:
                self.__path = os.path.join(self.__path, self.__file)

    @property
    def jsonfy(self):
        return self.__jsonfy

    @jsonfy.setter
    def jsonfy(self, value: bool):
        if type(value) == bool:
            self.__jsonfy = value

    @property
    def bucket(self):
        return self.__bucket

    @bucket.setter
    def bucket(self, value: list | tuple):
        s3_resource = boto3.resource('s3', aws_access_key_id=value[0],
                                     aws_secret_access_key=value[1])
        self.__bucket = s3_resource.Bucket(value[2])

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
      if type(value) == bool:
            self.__dir_erase = value

    @property
    def exception(self):
        return self.__exception

    @exception.setter
    def exception(self, value: str | list | tuple):
        self.__exception = value

    @staticmethod
    def is_json(content):
        '''
        only check if the content is already json

        '''
        try:
            json_object = json.loads(content)
        except ValueError as e:
            return False
        return True

    def download_all_files(self):
        '''
        as the name sugets, download files from bucket
        params: bucket, prefix, directory
        NOTE: other existing params it's for convenience
        bucket -- type list/tuple: connected to the bucket
        prefix -- type str: the path where you wish to download the files
        directory -- type str: the directory/path where you wish to download the files
        if directory is none
        default folder is bucket_files
        if doest not work erase the comment below the function probably it'll work
        '''

        self.__directory = os.path.join(os.getcwd(), self.__directory)
        try:
            os.chdir(self.__directory)
        except:
            os.mkdir(self.__directory)
        try:
            objects = self.__bucket.objects.filter(Prefix=f'{self.__prefix}/')
        except Exception:
            print('Prefix not found')
            return
        i = 0

        for object in objects:
            if not i == 0:
                file = object.key
                file = file.split('/')[1]
                if file.split('.')[1] not in self.__exception:
                    if self.__only_extension:
                        if file.split('.')[1] in self.__only_extension:
                            self.__bucket.objects.filter(
                                Prefix=f'{self.__prefix}/').download_file(file, self.__directory)
                    else:
                        self.__bucket.objects.filter(
                            Prefix=f'{self.__prefix}/').download_file(file, self.__directory)

            """
                s3_resource.Object(bucket_name, object.key).download_file(
                os.path.join(directory, file))
            """
            i += 1

    download_all_files(bucket=bucket, s3_resource=s3_resource,
                       prefix='Video', directory='master')

    def overwrite(self):
        '''
        same thing with the function  write(), but delete all previous content and place with the new one
        '''
        self.write(overwrite=True)

    def write(self, overwrite: bool = False):
        '''
        write the content on a file

        params: bucket, file, content, prefix, path, jsonfy, overwrite
        bucket -- well bucket is your bucket in aws which you must be connected.

        path -- type str: can be either relative path or absolute path to the folder, the path can contain the file too
        it will work in the following ways(only to exemple):

        Absolute path:
            C:\Windows\random_folder\
            C:\Windows\random_folder
            C:\Windows\random_folder\my_file.txt
            C:\Windows\random_folder\my_file.txt\
        Relative path (think you are at "C:\Windows\"):
            random_folder\my_file.txt
            random_folder\my_file.txt\
            random_folder\
            random_folder

        Note: IN CASE YOU DONT PROVIDE THE FILE AND IT EXTENSION IN THE PATH, YOU MUST PROVIDE IT IN the file param.

        file -- type str: can be just the file name with the sufix for exemple "my_file.txt" or could be None
        Note: file can only be empty if the path has the path+ file+ file_extension, exemple:
        C:\Windows\random_folder\my_file.txt\
        file exemple:
            my_file.txt
            my_file.txt\


        if path is none, it will try to find the file at the  default folder "bucket_files", if it fail your bad luck ;)
        jsonfy -- type bool:, by default it's False
            if it's true: convert the content to json format

        overwirte -- type bool:, by default it's False,
            if it's true: delete all previous content that existed in the file, and write with the current content being passed

        prefix -- type str: the path in your bucket where the file is (to be uploaded with the altered file)

        '''
        if self.__file and self.__path == None:
            print("path and file can't be none")
            return
        if not self.__file.split('.')[1] in Event.format_type or self.__path.split('.')[1] not in Event.format_type:
            if not self.__content:
                print('must provide a content')
                return

            else:
                if self.__path:
                    if len(self.__path.split('.')) > 1:
                        pass
                    else:
                        self.__path = os.path.join(
                            self.__path, self.__file)
                    if overwrite == True:
                        file = open(self.__path, 'w')
                    else:
                        file = open(self.__path, 'a')
                else:
                    if overwrite == True:
                        file = open(f'bucket_files\{self.__file}', 'w')
                    else:
                        file = open(f'bucket_files\{self.__file}', 'a')
                if self.__file.split('.')[-1] == 'json':  # check if it's json
                    json.dump(self.__content, file)
                else:
                    file.write(self.__content)
                file.close()
                try:
                    if self.__path:
                        if len(self.__path.split('.')) > 1:
                            upload_file = self.__path.split('\\')
                            upload_file = upload_file[-1]
                            self.__bucket.upload_file(self.__path,
                                                      f'{self.__prefix}\{upload_file}')
                        else:
                            self.__bucket.upload_file(self.__path,
                                                      f'{self.__prefix}\{self.__file}')
                    else:
                        self.__bucket.upload_file(os.path.join(os.path.getcwd, 'bucket_files'),
                                                  f'{self.__prefix}\{self.__file}')
                except Exception:
                    print('check file or path or prefix params')
        else:
            try:
                if self.__path:
                    if len(self.__path.split('.')) > 1:
                        upload_file = self.__path.split('\\')
                        upload_file = upload_file[-1]
                        self.__bucket.upload_file(self.__path,
                                                  f'{self.__prefix}\{upload_file}')
                    else:
                        self.__bucket.upload_file(self.__path,
                                                  f'{self.__prefix}\{self.__file}')
                else:
                    self.__bucket.upload_file(os.path.join(os.path.getcwd, 'bucket_files'),
                                              f'{self.__prefix}\{self.__file}')
            except Exception:
                print('check file or path or prefix params')
    # a = r"C:\Users\kayna\OneDrive\Área de Trabalho\aws\project_1\bucket_files"

    def read(self, jsonfy: bool = False):
        '''
        it will get the content of a file and return it to be used as you please
        params: file, path, jsonfy
        path -- type str: can be either relative path or absolute path to the folder, the path can contain the file too
        it will work in the following ways(only to exemple):

        Absolute path:
            C:\Windows\random_folder\
            C:\Windows\random_folder
            C:\Windows\random_folder\my_file.txt
            C:\Windows\random_folder\my_file.txt\
        Relative path (think you are at "C:\Windows\"):
            random_folder\my_file.txt
            random_folder\my_file.txt\
            random_folder\
            random_folder

        Note: IN CASE YOU DONT PROVIDE THE FILE AND IT EXTENSION IN THE PATH, YOU MUST PROVIDE IT IN the file param.

        file -- type str: can be just the file name with the sufix for exemple "my_file.txt" or could be None
        Note: file can only be empty if the path has the path+ file+ file_extension, exemple:       
        C:\Windows\random_folder\my_file.txt\
        file exemple:
            my_file.txt
            my_file.txt\


        if path is none, it will try to find the file at the  default folder "bucket_files", if it fail your bad luck ;)
        jsonfy -- type bolean: by default it's False
            if it's true: convert the content to json format
        '''
        if self.__file.split('.')[1] not in Event.format_type:
            if self.__path and self.__file == None:
                print("file and path can't be none")
                return
            if self.__path:
                if len(self.__path.split('.')) > 1:
                    try:
                        file = open(self.__path, 'r')
                    except:
                        print('check path again')
                else:
                    try:
                        file = open(os.path.join(
                            self.__path, self.__file), 'r')
                    except:
                        print('check path and file  again')
            else:
                try:
                    file = open(os.path.join(os.getcwd, 'bucket_files'), 'r')
                except Exception:
                    print('check path and file  again')
                    return
            if self.__file.split('.')[-1] == 'json':  # check if it's jason
                content = json.load(file)
                file.close()
                if self.__jsonfy or jsonfy == True:
                    if self.is_json(content) == False:
                        content = json.dumps(content)

                return content
            else:
                content = file.read()
                if self.__jsonfy or jsonfy == True:
                    if self.is_json(content) == False:
                        content = json.dumps(content)
                file.close()
                return print(content), content

    def erase(self, dir_erase: bool = False):
        '''
        erase files for you
        params: path, extension, dir_erase
        path --the relative path or absolute path to the folder where you with the delete the itens

        extension -- delete ALL the files with she same extension given 
            exemple:
            imagine you have in your folder the files
            --- my_file.txt
            --- my_bd.sql
            --- a_text.txt
            --- my_homework.pdf
            if you provide in your extension the value "txt" it will delete all itens with the "txt" extension,
            your folder would be like this
            --- my_bd.sql
            --- my_homework.pdf
        NOTE: if extension is none it will DELETE ALL FILES in the directory

        dir_erase -- boolean param, by default it's false, if true it will try to remove the folder(path) after deleting the files

        NOTE: it'll only work if after deleting the files, the folder is current empty
        '''
        if not self.__extension:
            os.chdir(self.__path)
            itens = os.listdir()
            for item in itens:
                os.remove(item)
            if self.__dir_erase or dir_erase == True:
                os.rmdir(self.__path)
        else:
            # walk list dir turbinado
            # os.getcwd(path)
            for root, dirs, file in os.walk(os.chdir(self.__path)):
                if file.split('.')[1] == self.__extension:
                    os.remove(item)
            if dir_erase == True:
                os.rmdir(self.path.split('\\')[-1])

    def events_to_do(self, events=[]):
        '''
        it get 'objects' to do actions
        it must be provided 
        aws_access_key_id
        aws_secret_acess_key
        bucket name
        the event to do exemple: write, overwrite, read, erase, download_all_files
        for each 'event' must be provided the necessary params to use it
        '''
        for item in events:
            if not item.event:
                print('Event is not valid')
            elif not item.file and item.path:
                print('No file or path Specified')
            else:
                try:

                    s3_resource = boto3.resource('s3',
                                                 aws_access_key_id=item.bucket[0],
                                                 aws_secret_access_key=item.bucket[1])
                    bucket = s3_resource.Bucket(item.bucket[2])

                    if item.event == 'write':
                        self.write(bucket=bucket,
                                   file=item.file,
                                   content=item.content,
                                   prefix=item.prefix,
                                   jsonfy=item.jsonfy)
                    elif item.event == 'overwrite':
                        if item.prefix == None:
                            self.overwrite(bucket=bucket,
                                           file=item.file,
                                           content=item.content,
                                           prefix='Video',
                                           jsonfy=item.jsonfy)
                        self.overwrite(bucket=bucket,
                                       file=item.file,
                                       content=item.content,
                                       prefix=item.prefix,
                                       jsonfy=item.jsonfy)
                    elif item.event == 'read':
                        self.read(file=item.file, jsonfy=item.jsonfy)
                    elif item.event == 'download_all_files':
                        self.download_all_files(bucket=bucket,
                                                s3_resource=s3_resource,
                                                prefix=item.prefix,
                                                directory=item.directory,
                                                bucket_name=item.bucket[2])

                    # elif item.event == 'download_all_files':
                    # download_all_files(bucket=bucket, s3_resource=s3_resource, prefix=item.prefix,directory=item.directory, bucket_name=item.bucket[2])
                    elif item.event == 'erase':
                        self.erase(path=item.path,
                                   extension=item.extension,
                                   dir_erase=item.dir_erase)
                    else:
                        'you suck'
                except:
                    print(
                        'File does Not exists at the Prefix informed or in base Prefix')

                else:
                    print('Success')

