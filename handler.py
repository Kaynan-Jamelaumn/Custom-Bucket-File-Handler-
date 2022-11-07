import boto3
import json
import os

format_type = [
  'png', 'jpg', 'tiff', 'gif', 'eps', 'svg', 'jpeg', 'bmp', 'psd', 'raw',
  'webp'
  'avi', 'mp4', 'm4v', 'mov', 'mpg', 'mpeg', 'wmv', 'aiff', 'au', 'mid',
  'midi', 'mp3', 'm4a', 'wav', 'wma'
]


def is_json(content):
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
      s3_resource.Object(bucket_name, object.key).download_file(
        os.path.join(directory, file))
    i += 1


# download_all_files(bucket=bucket, s3_resource=s3_resource,                  prefix='Video', directory='Fon')


def overwrite(bucket, file, content='', prefix='', path='', jsonfy=False):
  write(bucket, file, content, prefix, path, jsonfy, True)


def write(bucket,
          file='',
          content='',
          prefix='',
          path='',
          jsonfy=False,
          overwrite=False):
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


def events_to_do(events=[]):
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
          print('sdsds', item.bucket[2], item.prefix, item.directory)
          download_all_files(bucket=bucket,
                             s3_resource=s3_resource,
                             prefix=item.prefix,
                             directory=item.directory,
                             bucket_name=item.bucket[2])
        else:
          'you suck'
      except:
        print('File does Not exists at the Prefix informed or in base Prefix')

      else:
        print('Success')


def erase(path, extension='', dir_erase=False):
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
