import glacier_uploader as gl
import multiprocessing as mp
import botocore.exceptions
from os.path import split, getsize
from os import getcwd, system, listdir, remove
import boto3
from botocore.session import Session
from botocore.client import Config
import sys
import time
import yaml

AWS_CONF_PATH="conf/glacier_config.yaml"
config = Config(connect_timeout=50, read_timeout=70)
session = Session()
# There will be a line of debug log for this
print('\n\n running debug log\n')
#session.set_debug_logger()
print('\n\n'
)


hist = gl.get_history('history.p')
with open(AWS_CONF_PATH) as cfile:
    yaml.load(cfile)

def timer(start,end):
    hours, rem = divmod(end-start, 3600)
    minutes, seconds = divmod(rem, 60)
    return "{:0>2}:{:0>2}:{:05.8f}".format(int(hours),int(minutes),seconds)


# TODO: replace unix split with python version. parameters not consistent across platforms
def unxplit(filename, bytes_per_file, opref):
    system('split -a 4 -b {} {}'.format(str(bytes_per_file), filename))
    outfiles = [opref+f for f in listdir(getcwd()) if f.startswith(opref)]
    return outfiles


def partfeed(params):
                '''xpart is string of file name,
                xrange is tuple,
                checksum passed to this function as a string'''
                lobound = params['xrange'][0]
                ubound = params['xrange'][1]
                try:
                    part_response = gl.glacier_client.upload_multipart_part(vaultName=params['vault'],
                                                                 uploadId=params['upid'],
                                                                 body=open(params['xpart'], 'rb'),
                                                                 checksum=params['xthash'],
                                                                 range='bytes {}-{}/*'.format(lobound,
                                                                                              ubound),
                                                                 accountId=params['acctid'])
                    output = (lobound, ubound)
                except:
                    output = (0, 0)

                return output
upool = mp.Pool(processes=mp.cpu_count())


def mpUpload(fname,
             vault,
             desc,
             acctid,
             hrglass,
             size,
             pool=upool):

    '''Initiate a multipart upload, split file into parts, calculate checksums,
    treehash values, for each, and uploads to glacier in parallel according to # of cores in machine.'''
    startime = time.time()
    #chunkscheme = chunksize(fname)  # chunkscheme[0]= size of chunk
    fullsize = getsize(fname)
    #size, partcount = chunkscheme[0], chunkscheme[1]
    partcount = (fullsize//size) + 1
    #graph = ''
    try:
        if size < 42494967296:
            try:
                print('calling aws initiate function')
                try:
                    desc=desc.replace('*', '+')
                except:
                    pass
                mpu = gl.glacier_client.initiate_multipart_upload(vaultName=vault,
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
            filist = unxplit(fname, size, gl.PART_PREFIX)
            postplit = time.time() - presplit
            print('time: {}'.format(timer(presplit, time.time())))
            of = len(filist)


            #if of != partcount:
            #    print('of doesnt equal partcount...')
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
                               'acctid':gl.ACCOUNT_ID}
                num = filist.index(part)+1
                bytestring = open(part, 'rb')
                thash = str(gl.core.calculate_tree_hash(bytestring))
                # bytestring.close()
                part_params.update({'xthash':thash})
                bytestring.close()  # size is the closest power of two that is greater than the size of the part!
                btrange = totrange + getsize(part)
                prtrange = (totrange, btrange-1)
                part_params.update({'xrange':prtrange})
                all_params.append(part_params)
                comprop = prop
                prop = round((len(all_params)*100)/of)
                if prop != comprop:
                    print(bar(prop), ' {} of {}'.format(num, of))
                totrange += size # range is increasing by size, which is larger than the size of the part
            print('Uploading to Amazon Glacier...\n')
            # map part uploads into parallel scheme using upool
            ccount = 0
            errcount = 0
            done = round((ccount/of)*100)
            for partload in pool.imap_unordered(partfeed, all_params):  #  pool.imap() parallelization
                if not partload[1]:  #  upload part failed
                    print('failed to upload range {}'.format(partload[0]))
                    errcount += 1
                else:  # successful part upload
                    pdone = done
                    done = round((ccount/of)*100)
                    ccount += 1
                    if done != pdone:
                        print('byterange {} successfully uploaded'.format(partload[0]))
                        print(bar(done))
            print('{} of {} successfully uploaded'.format(ccount, of))
            print('{} failed parts'.format(errcount))
            if errcount != 0:
                #firstpass = time.time()-postplit
                print('time: {}'.format(timer(postplit, time.time())))
                print('retrying failed parts')
                uploaded = glacier_client.list_parts(vaultName=vault,
                                                     uploadId=upid)['Parts']
                while len(uploaded) < len(all_params):
                    #  check to see if all parts have uploaded

                    rangit = [i['RangeInBytes'] for i in uploaded]
                    checkparts = (tuple(byterange.split('-')) for byterange in rangit)  # received byteranges from AWS server
                    remains = (left for left in all_params if left['xrange'] not in checkparts)
                    uploaded = glacier_client.list_parts(vaultName=vault,
                                                     uploadId=upid)['Parts']


                    for partload in pool.imap_unordered(partfeed, remains):
                        if partload[1] == 0:
                            print('failed to upload range: {}'.format(partload))
                            errcount += 1
                        else:
                            print('bytes {} successfully uploaded!'.format(partload))
                            ccount += 1
                            print(bar(round((ccount/of)*100)))

            # close pool
            pool.close()
            pool.join()  # retry any uploads left over
            with open(fname, 'rb') as f:
                full_tree_hash = core.calculate_tree_hash(f)
                f.close()
            pool.terminate()
            completion = glacier_client.complete_multipart_upload(vaultName=vault,
                                                          uploadId=upid,
                                                          archiveSize=str(fullsize),
                                                          checksum=full_tree_hash,
                                                          accountId=acctid)
            # trash(PART_PREFIX)
            print('\nMultipart Upload of Archive: {} to Vault {} Completed\n'.format(fname,
                                                                             vault))
            # for safety, * is marker string for history update
            output = {'FileName':fname,
                  'Description':"{}: {} *{}* ".format(completion['ResponseMetadata']['HTTPHeaders']['date'],
                                                desc, fname),
                  'VaultName': vault,
                  'ArchiveId': completion['archiveId'],
                      'Size': str(fullsize)}
            return output
        else:  # this needs to happen before the success message if
            print('Amazon does not support archive files over 40 GB, {} is {}'.format(fname, gl.size_display(fullsize)))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        name = split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('Python raised the following excception: {} {} {} {}'.format(exc_type, e, name, exc_tb.tb_lineno))
        gl.glacier_client.abort_multipart_upload(vaultName=vault,
                                              upload_id=upid)
    finally:
        gl.trash(gl.PART_PREFIX)
        return None



testrange = [2**i for i in range(20, 32)]

def speed_test(testfile, domain):
    for i in domain:
        mpUpload(fname=testfile, vault='FrontEndTest', )
