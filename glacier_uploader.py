#!/usr/bin/python3
# IcePyck interface for Amazon Glacier

from os import system, listdir, getcwd, remove
from os.path import getsize, split
import math
from boto3 import client, resource   #  need only to import client, resource
from hashlib import sha256
import botocore.utils as core
import botocore.exceptions
import pickle as p      #  pickle to store persistent data # TODO: change to protobuf
import time
from botocore.session import Session
from botocore.client import Config
from multiprocessing import Pool, cpu_count
import sys
import random
import yaml

AWS_CONF_PATH='conf/glacier_config.yaml'
# read config params from yaml
with open(AWS_CONF_PATH) as cfile:
    params = yaml.safe_load(cfile)
config = Config(connect_timeout=100, read_timeout=100)
glacier_client = client('glacier',
                aws_access_key_id=params['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=params['AWS_SECRET_ACCESS_KEY'],
                              config=config)
session = Session()
config = Config(connect_timeout=100, read_timeout=100)
glacier = resource('glacier')
sns_client = client('sns')
PART_PREFIX = 'part_'
BYT_PREFIX = 'byte_'

def randbool(prob=.25):
    '''generates a random boolean with probability as a parameter with default 1/4'''
    event = random.random()
    if event <= prob:
        return True
    else:
        return False

def partfeed(params):
    lobound = params['xrange'][0]
    ubound = params['xrange'][1]
    try:
        if randbool():  #  this simulates random exceptions TAKE OUT WHEN DONE TESTING!
            raise Exception
        glacier_client.upload_multipart_part(vaultName=params['vault'],
                                                     uploadId=params['upid'],
                                                     body=open(params['xpart'], 'rb'),
                                                     checksum=params['xthash'],
                                                     range='bytes {}-{}/*'.format(lobound,
                                                                                  ubound),
                                                     accountId=params['acctid'])

        output = lobound, ubound, True

    except:
        output = lobound, ubound, False
    return output

#upool=Pool(processes=cpu_count())
#rpool = Pool(processes=cpu_count())

def make_out_filename(prefix, idx):
    '''Make a filename with a serial number suffix.'''
    return prefix + str(idx).zfill(4)

def bytesize(imp):
    return 2**(math.ceil(math.log(imp, 2)))  #  check this out later

def chunksize(file):
    '''returns a tuple of 2 integers chunksize(file)[0] is the size of an upload segment
    chunksize(file)[1] is the number of reqired file part'''
    fulsize = getsize(file)
    if fulsize >= 16777216: # 201326592:  # 192mb
        chunk = 4194304 # bytesize(math.ceil(fulsize/9999))      #67108864   # 64mb
    #elif fulsize >= 100663296:  # 96mb
    #    chunk = 33554432  # 32mb
    #elif fulsize >= 50331648:  # 48mb
    #    chunk = 16777216  # 16mb
    #else:   #fulsize <= 166777216:
    #    chunk = fulsize
    elif fulsize >=1048576:
        chunk = 1048576
    else:
        chunk = fulsize
    return bytesize(chunk), math.ceil(fulsize/chunk)

def read_in_chunks(file, chunksize=3):
    while True:
        data = file.read(chunksize)
        if not data:
            break
        yield data



def unxplit(filename, bytes_per_file, opref, tdir="tmp/"):
    system("split -a 4 -b {} '{}'".format(str(bytes_per_file), filename))
    outfiles = [tdir+opref+f for f in listdir(tdir) if f.startswith(opref)]
    return outfiles

def trash(prefix):
    '''deletes all files with a certain prefix'''
    path = getcwd()
    dirlog = listdir(path)
    for filename in dirlog:
        if filename.startswith(prefix):
            remove(filename)


def shash(chunk):
    '''returns sha256 hash object from string'''
    #file2hash = chunk
    bank = sha256()
    bank.update(chunk)
    return bank.digest()

def timer(start,end):
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.8f}".format(int(hours),int(minutes),seconds)

def bar(n):
    '''prints download bar corresponding to a given number as percent between 1 and 100'''
    chash = ((60*round(n))//100)
    output = "[{}{}] {}%".format('#' * chash, ' ' * (60-chash), round(n, 2))
    return output



def mpUpload(fname,
             vault,
             desc,
             acctid,
             hrglass):

    '''Initiate a multipart upload, split file into parts, calculate checksums,
    treehash values, for each, and uploads to glacier in parallel according to # of cores in machine.'''
    startime = time.time()
    chunkscheme = chunksize(fname)  # chunkscheme[0]= size of chunk
    fullsize = getsize(fname)
    size, partcount = chunkscheme[0], chunkscheme[1]
    #graph = ''
    try:
        if size < 42494967296:  # this is not 40gb
            try:
                print('calling aws initiate function')
                try:
                    desc = desc.replace('*', '+')
                except:
                    pass
                mpu = glacier_client.initiate_multipart_upload(vaultName=vault,
                                                   archiveDescription=desc,
                                                   partSize=str(size))
            except botocore.exceptions.ParamValidationError or botocore.exceptions.ClientError:
                print('Multipart Upload operation failed, {} not uploaded to {}'.format(fname,
                                                                                        vault))
                return None
            upid = mpu['uploadId']
            print('\nMultipart Upload initiated for {} to {}\n Upload Request ID: {}'.format(fname, vault, upid))
            try:
                print(hrglass)
            except:
                pass
            print('This may take a while, {} is {} watching it happen is not recommended...\n'.format(fname, size_display(fullsize)))
            print('\nsplitting input file into {} parts\n'.format(partcount))
            presplit = time.time()-startime
            print('time {}'.format(timer(startime, time.time())))
            filist = unxplit(fname, size, PART_PREFIX)
            postplit = time.time() - presplit
            print('time: {}'.format(timer(postplit, time.time())))
            of = len(filist)


            if of != partcount:
                print('of: {} doesnt equal partcount: {}...'.format(of, partcount))
                trash(PART_PREFIX)
                sys.exit()

            totrange = 0
            #response = []

            all_params = []

            #  iterate over each file part to compile parameters
            print('Compiling parameters for upload segments...')
            prop = 0
            for part in filist:
                part_params = {'xpart':part,
                               'vault':vault,
                               'upid':upid,
                               'acctid':ACCOUNT_ID}
                num = filist.index(part)+1
                bytestring = open(part, 'rb')
                thash = str(core.calculate_tree_hash(bytestring))
                part_params.update({'xthash':thash})
                bytestring.close()  # size is the closest power of two that is greater than the size of the part!
                btrange = totrange + getsize(part)
                prtrange = (totrange, btrange-1)
                part_params.update({'xrange': prtrange})
                all_params.append(part_params)

                # display progress
                comprop = prop
                prop = (len(all_params)*100)/of
                if prop != comprop:
                    print(bar(prop), ' {} of {}'.format(num, of))
                # increment how much of the file has now been accounted for
                totrange += size  # range is increasing by size, which is larger than the size of the part
            print('Uploading to Amazon Glacier...\n')

            # map part uploads into parallel scheme using upool
            ccount = 0
            errcount = 0
            done = round((ccount/of)*100) #  done refers to the percentage
            pool = Pool(processes=cpu_count())
            for partload in pool.imap_unordered(partfeed, all_params):  #  pool.imap() parallelization
                if not partload[2]:  #  upload part failed
                    print('failed to upload range {}-{}'.format(partload[0], partload[1]))
                    errcount += 1  # make errcount a vector of the failed parameters
                else:  # successful part upload
                    pdone = done
                    ccount += 1
                    done = (ccount/of)*100
                    if done != pdone:
                        print('byterange {}-{} successfully uploaded'.format(partload[0], partload[1]))
                        print(bar(done))
            print('{} of {} successfully uploaded'.format(ccount, of))
            pool.close()
            pool.join()
            pool.terminate()
            pool = Pool(processes=cpu_count())
            print('{} failed parts'.format(errcount))
            if errcount != 0:
                #firstpass = time.time()-postplit
                errcount = 0

                uploaded = glacier_client.list_parts(vaultName=vault,
                                                     uploadId=upid)['Parts']
                while len(uploaded) < len(all_params):
                    #  check to see if all parts have uploaded
                    print('time: {}'.format(timer(postplit, time.time())))
                    postplit = time.time()
                    errcount = 0
                    print('retrying {} failed parts'.format(errcount)) # no .format here
                    rangit = [i['RangeInBytes'] for i in uploaded]
                    checkparts = (tuple(byterange.split('-')) for byterange in rangit)  # received byteranges from AWS server
                    rcheckparts = [(int(ad[0]), int(ad[1])) for ad in checkparts]
                    remains = [left for left in all_params if left['xrange'] not in rcheckparts]

                    for partload in pool.imap(partfeed, remains):
                        if not partload[2]:
                            print('failed to upload range: {}-{}'.format(partload[0], partload[1]))
                            errcount += 1
                        else:
                            ccount += 1
                            print('bytes {}-{} (part {} of {}) successfully uploaded!'.format(partload[0],
                                                                                              partload[1],
                                                                                              ccount,of))
                            print(bar(round((ccount/of)*100)))
                    print('{} of {} successfully uploaded'.format(ccount, of))
                    print('{} failed parts'.format(errcount))
                    pool.close()
                    pool.join()
                    uploaded = glacier_client.list_parts(vaultName=vault,
                                                     uploadId=upid)['Parts']
                    pool.terminate()
                    pool = Pool(processes=cpu_count())


            # close pool
            #pool.close()
            #pool.join()  # retry any uploads left over
            with open(fname, 'rb') as f:
                full_tree_hash = core.calculate_tree_hash(f)
                f.close()

            completion = glacier_client.complete_multipart_upload(vaultName=vault,
                                                          uploadId=upid,
                                                          archiveSize=str(fullsize),
                                                          checksum=full_tree_hash,
                                                          accountId=acctid)
            # trash(PART_PREFIX)
            print('\nMultipart Upload of Archive: {} to Vault {} Completed\n'.format(fname,
                                                                             vault))
            pool.terminate()
            # for safety, * is marker string for history update
            output = {'FileName':fname,
                  'Description':"{}: {} *{}* ".format(completion['ResponseMetadata']['HTTPHeaders']['date'],
                                                desc, fname),
                  'VaultName': vault,
                  'ArchiveId': completion['archiveId'],
                      'Size': str(fullsize)}
            return output
        else:  # this needs to happen before the success message if
            print('Amazon does not support archive files over 40 GB, {} is {}'.format(fname, size_display(fullsize)))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        name = split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('Python raised the following excception: {} {} {} {}'.format(exc_type, e, name, exc_tb.tb_lineno))
        glacier_client.abort_multipart_upload(vaultName=vault,
                                              upload_id=upid)
    finally:
        trash(PART_PREFIX)
        return None


def new_vault(name):

    '''Creates a new vault in Amazon Glacier'''

    response = glacier_client.create_vault(accountId='-',
                                   vaultName=name)
    return response


def delete_vault(vault):
    '''deletes an existing vault'''
    glacier_client.delete_vault(vaultName=vault)

def inventory_reader(bstring):
    '''translates vault-inventory csv's to python array of dictionaries'''
    output = []
    entries = bstring.decode().split('\n')
    keys = entries[0]
    keys=keys.split(',')
    entries.pop(0)
    try:
        entries.remove('')
    except:
        pass
    entries = [i.split(',') for i in entries]
    for entry in entries:
        entput = {}
        for key in range(5):
            k=len(entry)
            if k == 5:
                entput.update({keys[key]: entry[key]})
            else:  #  these had commas in their description, string must be repaired
                entry[1]=','.join(entry[1:k-3])
                entry = entry[0:2]+entry[2:k-4]

        output.append(entput)
    return output






def retrieve(job, arc=False, fname=None):
    '''checks for completion of a given job and retrieves it
    once the job has been completed'''
    try:
        vault = job.vault_name
        print('Retrieval Request pending, Waiting on Amazon Servers'
              '\nThis can take 3 to 8 hours from request time')
        initime = time.time()
        while job.status_code =='InProgress':
            time.sleep(600)
            elapsed=time.time()-initime
            print('retrieval request pending from {}, time elapsed {}'.format(vault, elapsed))
        if job.status_code == 'Failed':
            print('Something went wrong server side, check AWS dashboard')
        else:
            outfile = open('{}'.format(fname), 'wb')
            try:
                botout = job.get_output()
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                name = split(exc_tb.tb_frame.f_code.co_filename)[1]
                outfile.write(bytes('Boto3 raised the following excception: {} {} {} {}'.format(exc_type, e, name, exc_tb.tb_lineno)))
                outfile.close()
                return None
            outbytes = botout['body'].read()

            if not arc:
                outbytes = inventory_reader(outbytes)
            print('saving file to IcePick/retrievals/')
            outfile.write(outbytes)
            outfile.close()


    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        name = split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('Python raised the following excception: {} {} {}; line {}'.format(exc_type, ex, name, exc_tb.tb_lineno))
        return None

def retrieval_request(vault,
                      retrlog,
                      arcid=None,
                        email=None,
                        phone=None,
                        arc=False):

    '''requests an inventory from a vault given as a parameter'''

    #if email is None and phone is None:
    #    raise ValueError('must specify at least one notification channel')
    if arc is False and arcid is not None:
        raise SyntaxError('No archive id to be specified for inventory upload')

    #  determine request type
    if arc is True:
        request_type = 'archive_request'
    else:
        request_type = 'inventory'

#  request notification platform from user and creates an sns topic to which entered platforms are subscribed
    topic_name = '{}_{}'.format(vault, request_type)
    sns_topic = sns_client.create_topic(Name=topic_name)
    info = {'email': email,
            'sms': phone}
    topstring = '{}'.format(sns_topic['TopicArn'])
    for protocol in info.keys():  # each protocol: email, sms
        if info[protocol] is None:  # if an email address or phone number has not been entered by the user
            pass
        else:
            try:  # make an sns subscription with the information entered
                sns_client.subscribe(TopicArn=sns_topic['TopicArn'],
                                     Protocol=protocol,
                                     Endpoint=info[protocol])
                print('{} protocol registered to sns topic'.format(protocol))
            except:   # print an error message regarding that information
                print('an error occurred in registering {} protocol'.format(protocol))

# initiate request
    if arc is False:  # in the case of an inventory request:
        params = {'Type': 'inventory-retrieval',
             'Description': 'inventory retrieval for vault {}'.format(vault),
             'Format': 'CSV', 'SNSTopic': topstring}

    else:  # in the case of an archive request:
        params= {'Type': 'archive-retrieval',
             'Description': 'archive retrieval job for {}'.format(vault),
             'ArchiveId': arcid,
                 'SNSTopic': topstring}  # test version

# execute boto3 call to initiate request
    doc = glacier_client.initiate_job(accountId='-',
                          vaultName=vault,
                          jobParameters=params)
    doc.update({'vault': vault})
    jobid=doc['jobId']
#  updates retrieval log with the documentation of the current request
    retrlog.append(doc)

# notify user of completion
    job = glacier.Job(id=jobid, vault_name=vault, account_id=params['ACCOUNT_ID'])
    print('retrieval request received, job ID: {}'.format(doc['jobId']))
    print("""{} should appear in the folder IcePick/retrievals within
    the next 3-12 hours. Keep this window open, Icepick is checking the
    status of this retrieval periodically in the background""")

    return doc, job



def delete_archive(vaultname, archid):
    try:
        glacier_client.delete_archive(archiveId=archid,
                          vaultName=vaultname)
    except botocore.exceptions.ClientError:
        print("couldn't delete, Try ")

def get_archive(jobid, retrlog):
    return None

def job_status(jobname, retrlog):
    '''gets status of retrieval job'''
    try:
        info = glacier_client.describe_job(vaultName=jobname[3:-1],
                                       jobId=retrlog[jobname][0])
    except:
        print('Job {} not found'.format(jobname))
        return None


def size_display(bytecount):
    if not bytecount >= 1024: # display count in bytes
        count = bytecount
        out = '{} bytes'.format(count)
    elif not bytecount >= 1048576:
        count = round(bytecount/1024, 2) #  display count in KB
        out = '{} KB'.format(count)
    elif not bytecount >= 1073741824:
        count = round(bytecount/1048576, 2)  # display count in MB
        out = '{} MB'.format(count)
    else:
        count = round(bytecount/1073741824, 2)  #  display count in GB
        out = '{} GB'.format(count)
    return out


def vaultextract(address):
    '''returns vault from glacier responsemetadata location'''
    start = address.split('/')
    pos = start.index('vaults')+1
    return start[pos]

def show_request_history(retrlog):

    '''prints history of job requests'''

    for key in retrlog.keys():
        status = glacier_client.describe_job()  #  retrhist needs new format: list of dicts with keys:
                                                        #  Vaultname, JobId, Name...




def list_vaults(history, display=False):
    list_info=glacier_client.list_vaults()["VaultList"]
    names = [i["VaultName"] for i in list_info]
    if display is True:   # prints for interface when display parameter is set to True
        print('\nCurrent Vaults:\n')
        for name in names:
            print('{}:\n'.format(name))
            invault = []
            if len(history) > 0:
                for entry in history:       #  try if len(history is not 0:
                    if entry['VaultName'] != name:   #  maybe not .lower()
                            pass
                    else:
                        invault.append(entry)
                        if len(invault) > 0:
                            for mem in invault:  #  if len(invault) > 0
                                for key in sorted(mem.keys()):
                                    print('{}: {}'.format(key,
                                                          mem[key]))
                        else:  # indent out!
                            print('this vault is empty')
                        print('\n')


    return names

def reporter(uploadout,
             vaultname,
             old_history):
    '''convert from AWS vault inventory format to IcePick history format'''
    history = {}
    for i in old_history:
        history.update(i)
    for entry in uploadout:
        if len(entry) == 0:
            pass
        else:
            arcid = entry['ArchiveId']
            desc = entry['ArchiveDescription']
            try:
                fname = desc.split('*')[1]

            except IndexError:
                print("couldn't get file name for archiveid: {}".format(arcid))
                fname = 'Unknown'
            try:
                size=entry['Size']

            except KeyError:
                print('Could not get size format for archiveid: {}'.format(arcid))
                size = 'Unknown'

            arcid = entry['ArchiveId']
            new_entry = {'ArchiveId':arcid,
                         'Description':desc,
                         'FileName':fname,
                         'VaultName':vaultname,
                         'Size':size}
            history.update(new_entry)
    return history




def get_history(pickname dp="tmp/"):
    try:
        historiography = open(dp+pickname, 'rb')   # historiography ('history.p') is pickle
    except FileNotFoundError:                       # data file containing a python dictionary containing
        print('No prior history found for {}'.format(pickname))  
        historiography.close()              # request history data organized by file name
        history = []
        return history

    try:
        history = p.load(historiography)  #  Unpickle history from historiography file
        historiography.close()              #  Close historiography file
        return history

    except EOFError:
        print('history file was unreadable.')
        historiography.close()
        history = []
        return history


def update_history(infilename, history):
    dcode = inventory_reader(open(infilename, 'rb').read())
    newdat = reporter(dcode, infilename, history)
    for i in newdat:
        print(i)

def dict_print(history, title):
    print('{:^}\n'.format(title))
    namedate = {}
    for key, value in history.items():
        date = value['HTTPHeaders']['date']
        archid = value['HTTPHeaders']['x-amz-archive-id']
        namedate.update({key: date})
        info = """{}
        {}""".format(date,
                     archid)
        for val in namedate.keys():
            print()


def save_history(history, filename):
    outfile = open(filename, 'wb')
    p.dump(history, outfile)
    outfile.close()



hstitle = 'Amazon Glacier Request History:'


if __name__ == '__main__':
    hst = get_history('history.p')
    retrhist = get_history('retrhist.p')
    print('\n\n')

    list_vaults(hst, display=True)

    print('\n')

    #this = mpUpload('LargeTestFile', 'FrontEndTest', 'independent mp_upload_test', '-', 'Its loading')


    #update_history(infilename='FrontEndTest', history=hst)

    # test mpUpload
    fooname = 'capital_vol1_0810_librivox_64kb_mp3.zip'
    barname = 'degroot-schervish-2012.pdf'
    omname = 'LargeTestFile'
    testvault = 'FrontEndTest'


    mpUpload(fname=fooname,acctid=params['ACCOUNT_ID'],hrglass=':-)',vault=testvault, desc='test for upload correction scheme XII-medfile')
    #job = glacier.Job(account_id=ACCOUNT_ID, id='hCyAbBooy0A3b4NsEzdxijBWmnEpzAC563RHqhcUu-_uZKN-CZhZLtaxM2_1Gkbqf28yqamJXSS0X6L_Mq9RAWxdF1uY',
    #                  vault_name='FrontEndTest')
    #this = retrieve(job=job,arc=True,fname='degroot-retrieved.pdf')




