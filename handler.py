import boto3
import json
import os

format_type = [
  'png', 'jpg', 'tiff', 'gif', 'eps', 'svg', 'jpeg', 'bmp', 'psd', 'raw',
  'webp', 'avi', 'mp4', 'm4v', 'mov', 'mpg', 'mpeg', 'wmv', 'aiff', 'au',
  'mid', 'midi', 'mp3', 'm4a', 'wav', 'wma'
]


def is_json(content):
  '''
    only check if the content is already json 
  
  '''
  try:
    json_object = json.loads(content)
  except ValueError as e:
    return False
  return True


def download_all_files(bucket,
                       s3_resource,
                       prefix,
                       directory='',
                       bucket_name=''):
  '''
    as the name sugets, download files from bucket
    params: bucket, prefix, directory 
    NOTE: other existing params it's for convenience

    bucket -- connected to the bucket
    prefix -- the path where you wish to download the files
    directory -- the directory/path where you wish to download the files
    if directory is none
    default folder is bucket_files

    if doest not work erase the comment below the function probably it'll work
  '''
  if not directory:
    directory = 'bucket_files'
  directory = os.path.join(os.getcwd(), directory)
  try:
    os.chdir(directory)
  except:
    os.mkdir(directory)
  objects = bucket.objects.filter(Prefix=f'{prefix}/')
  i = 0
  for object in objects:
    if not i == 0:
      file = object.key
      file = file.split('/')[1]
      bucket.objects.filter(Prefix=f'{prefix}/').download_file(file, directory)
      """
      s3_resource.Object(bucket_name, object.key).download_file(
        os.path.join(directory, file))
      """
    i += 1


# download_all_files(bucket=bucket, s3_resource=s3_resource,                  prefix='Video', directory='Fon')


def overwrite(bucket, file, content='', prefix='', path='', jsonfy=False):
  '''
    same thing with the function  write(), but delete all previous content and place with the new one
  '''
  write(bucket, file, content, prefix, path, jsonfy, True)


def write(bucket,
          file='',
          content='',
          prefix='',
          path='',
          jsonfy=False,
          overwrite=False):
  '''
    write the content on a file
    
    params: bucket, file, content, prefix, path, jsonfy, overwrite

    bucket -- well bucket is your bucket in aws which you must be connected.
    
    path -- can be either relative path or absolute path to the folder, the path can contain the file too
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

    
    file can be just the file name with the sufix for exemple "my_file.txt" or could be None
    Note: file can only be empty if the path has the path+ file+ file_extension, exemple:       
    C:\Windows\random_folder\my_file.txt\
    file exemple:
      my_file.txt
      my_file.txt\
      
    
    if path is none, it will try to find the file at the  default folder "bucket_files", if it fail your bad luck ;)

    jsonfy -- it's bolean, by default it's False
        if it's true: convert the content to json format
        
    overwirte -- it's boolean, by default it's False,
        if it's true: delete all previous content that existed in the file, and write with the current content being passed
        
    prefix -- the path in your bucket where the file is (to be uploaded with the altered file)
    
  '''
  if file and path == None:
    print('path and file cant be none')
    return
  if content or file.split('.')[1] not in format_type:
    upload_file = file  # name of the file
    if not file.split('.')[1] in format_type:
      if jsonfy == True:
        if is_json(content) == False:
          content = json.dumps(content)  # convert the lines to json
    if path:
      if file:
        if len(path.split('.')) > 1:
          # note: i prefere to not use "with" to open a file because it's more legible
          try:
            if not file.split('.')[1] in format_type:
              if overwrite == True:
                file = open(path, 'w')
              else:
                file = open(path, 'a')
          except:
            print('check path again')
        else:
          if file[0] == '\\':
            file = file[1:]
          try:
            if not file.split('.')[1] in format_type:
              if overwrite == True:
                file = open(os.path.join(path, file), 'a')
              else:
                file = open(os.path.join(path, file), 'w')
          except:
            print('1 file does not exists at the specified path')
      else:
        try:
          if not file.split('.')[1] in format_type:
            if overwrite == True:
              file = open(path, 'w')
            else:
              file = open(path, 'a')
        except:
          print('2 file does not exists at the specified path')
    else:
      # bucket files is the index directory to check if there's a file
      if not file.split('.')[1] in format_type:
        if overwrite == True:
          file = open(f'bucket_files\{file}', 'w')
        else:
          file = open(f'bucket_files\{file}', 'a')
    if not file.split('.')[1] in format_type:
      if upload_file.split('.')[-1] == 'json':  # check if it's jason
        json.dump(content, file)
      else:
        file.write(content)
      file.close()
    if len(path.split('.')) > 1:
      try:
        pathload = path.split('\\')
        if '.' in pathload[-2]:
          upload_file = pathload[-2]
        else:
          upload_file = pathload[-1]
        bucket.upload_file(path, f'{prefix}{upload_file}')
      except:
        print(
          '1 file does not exists at the specified path or prefix ir wrong')
    elif len(path.split('.')) == 0:
      try:
        bucket.upload_file(os.path.join(path, upload_file),
                           f'{prefix}\{upload_file}')
      except:
        print('3 path and file does not match or prefix ir wrong')
    else:
      try:
        bucket.upload_file(
          os.path.join(os.getcwd(), upload_file),
          f'{prefix}\{upload_file}')  # os.getcwd() get the current directory
      except:
        try:
          bucket.upload_file(
            f'bucket_files\{upload_file}',
            f'{prefix}\{upload_file}')  # generic folder that search file
        except:
          print("there's fuck")
  else:
    print('must have a content')


# a = r"C:\Users\kayna\OneDrive\Área de Trabalho\aws\project_1\bucket_files"


def read(file='', path='', jsonfy=False):
  '''
    it will get the content of a file and return it to be used as you please
    params: file, path, jsonfy

    path -- can be either relative path or absolute path to the folder, the path can contain the file too
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

    
    file can be just the file name with the sufix for exemple "my_file.txt" or could be None
    Note: file can only be empty if the path has the path+ file+ file_extension, exemple:       
    C:\Windows\random_folder\my_file.txt\
    file exemple:
      my_file.txt
      my_file.txt\
      
    
    if path is none, it will try to find the file at the  default folder "bucket_files", if it fail your bad luck ;)

    jsonfy -- it's bolean, by default it's False
        if it's true: convert the content to json format
  '''
  if file.split('.')[1] not in format_type:
    if path:
      if file:
        if len(path.split('.')) > 1:
          # note: i prefere to not use "with" to open a file because it's more legible
          try:
            read_file = path
            file = open(path, 'r')
          except:
            print('check path again')
        else:
          if file[0] == '\\':
            file = file[1:]
          try:
            read_file = os.path.join(path, file)
            file = open(os.path.join(path, file), 'r')
          except:
            print('1 file does not exists at the specified path')
      else:
        try:
          read_file = path
          file = open(path, 'r')
        except:
          print('2 file does not exists at the specified path')
    else:
      # bucket files is the index directory to check if there's a file
      read_file = file
      file = open(f'bucket_files\{file}', 'r')

    if read_file.split('.')[-1] == 'json':  # check if it's jason
      content = json.load(file)
      file.close()
      if jsonfy == True:
        if is_json(content) == False:
          content = json.dumps(content)

      return content
    else:
      content = file.read()
      if jsonfy == True:
        if is_json(content) == False:
          content = json.dumps(content)

      file.close()
      return print(content), content


def erase(path, extension='', dir_erase=False):
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
  if not extension:
    os.chdir(path)
    itens = os.listdir()
    for item in itens:
      os.remove(item)
    if dir_erase == True:
      os.rmdir(path)
  else:
    # walk list dir turbinado
    for root, dirs, file in os.walk(os.chdir(path)):  #os.getcwd(path)
      if file.split('.')[1] == extension:
        os.remove(item)
    if dir_erase == True:
      os.rmdir(path)


def events_to_do(events=[]):
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
          write(bucket=bucket,
                file=item.file,
                content=item.content,
                prefix=item.prefix,
                jsonfy=item.jsonfy)
        elif item.event == 'overwrite':
          if item.prefix == None:
            overwrite(bucket=bucket,
                      file=item.file,
                      content=item.content,
                      prefix='Video',
                      jsonfy=item.jsonfy)
          overwrite(bucket=bucket,
                    file=item.file,
                    content=item.content,
                    prefix=item.prefix,
                    jsonfy=item.jsonfy)
        elif item.event == 'read':
          read(file=item.file, jsonfy=item.jsonfy)
        elif item.event == 'download_all_files':
          download_all_files(bucket=bucket,
                             s3_resource=s3_resource,
                             prefix=item.prefix,
                             directory=item.directory,
                             bucket_name=item.bucket[2])

        #elif item.event == 'download_all_files':
        #download_all_files(bucket=bucket, s3_resource=s3_resource, prefix=item.prefix,directory=item.directory, bucket_name=item.bucket[2])
        elif item.event == 'erase':
          erase(path=item.path,
                extension=item.extension,
                dir_erase=item.dir_erase)
        else:
          'you suck'
      except:
        print('File does Not exists at the Prefix informed or in base Prefix')

      else:
        print('Success')
