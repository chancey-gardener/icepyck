#!/usr/bin/python3
# archive and inventory retrieval for IcePick
# Gopnik Lab, University of California, Berkeley

import time
import boto3
import os

glacier = boto3.resource('glacier')


def retrieve(jobid):
    '''checks for completion of a given job and retrieves it
    once the job has been completed'''
    job = boto3.Job(id=jobid,
                    )
    while job.status_code =='InProgress':
        time.sleep(600)
    if job.status_code == 'Failed':
        print('Something went wrong server side, check AWS dashboard')

    else:
        try:
            botout = job.get_output()
            outbytes = botout['body'].read()
            file = open('filename', 'wb')
            file.write(outbytes)

        except:
            return None


def bsplit(in_filename, bytes_per_file, opref, log=False):

    '''Split the input file in_filename into output files of
    bytes_per_file bytes each. Last file may have less bytes.'''

    in_fil = open(in_filename, "rb")
    outfil_idx = 1
    out_filename = make_out_filename(opref, outfil_idx)
    out_fil = open(out_filename, "wb")

    byte_count = tot_byte_count = file_count = 0
    c = in_fil.read(1)
    # Loop over the input and split it into multiple files
    # of bytes_per_file bytes each (except possibly for the
    # last file, which may have less bytes.
    chunklist=[]
    while len(c) != 0:
        byte_count += 1
        out_fil.write(c)
        # Bump vars; change to next output file.
        if byte_count >= bytes_per_file:
            tot_byte_count += byte_count
            byte_count = 0
            file_count += 1
            if log is True:
                chunklist.append(out_fil.name)
            out_fil.close()
            outfil_idx += 1
            out_filename = make_out_filename(opref, outfil_idx)
            out_fil = open(out_filename, "wb")
        c = in_fil.read(1)
    # Clean up.
    in_fil.close()
    if not out_fil.closed:
        out_fil.close()
    if byte_count == 0:
        os.remove(out_filename)
    if log is True:
        chunklist.append(out_fil.name)
        return chunklist  # does not add



def treehash(part):

    '''Computes sha256 hash of 2 file segments, concatenates them
    and computes sha256 of that'''
    #  can we define this recursively?

    byteparts = [shash(open(m).read().encode()) for m in bsplit(part,
                                                                1048576,
                                                                BYT_PREFIX,
                                                                log=True)]
    byte2byte = []  # represents one level up in hash tree
    while len(byteparts) > 1:
        for byte in range(1, len(byteparts), 2):
            if byte >= len(byteparts):  # unless byte is last in sequence
                twobyte = byteparts[byte] + byteparts[byte+1]  # concatenate current
                byte2byte.append(twobyte)                           # and subsequent byte
            else:                                                       # and append it to the vector byte2byte
                byte2byte.append(byteparts[byte])  #   append any odd object left out to byte2byte
        byteparts = [(shash(str(p).encode())) for p in byte2byte] #  byteparts becomes the hashes of the concatenated
                                                                    #  elements of byte2byte
        byte2byte = []  #  byte2byte cleared for next iteration
    root = shash(byteparts[0])
    trash(BYT_PREFIX)  #  clean up excess single megabyte files
    return root

def sermpUpload(fname,
             vault,
             desc,
             acctid,
             hrglass):

    '''Initiate a multipart upload, split file into parts, calculate checksums,
    treehash values, for each, and uploads to glacier.'''

    chunkscheme = gl.chunksize(fname)  # chunkscheme[0]= size of chunk
    fullsize = os.path.getsize(fname)
    size, partcount = chunkscheme[0], chunkscheme[1]
    graph = ''
    try:
        if size < 42494967296:
            try:
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
            print('This may take a while, {} is {} watching it happen is not recommended...\n'.format(fname, gl.size_display(fullsize)))
            print('\nsplitting input file into {} parts\n'.format(partcount))
            filist = unxplit(fname, size, gl.PART_PREFIX)
            of = len(filist)
            if of != partcount:
                print('of doesnt equal partcount...')
            totrange = 0
            response = []
            print('Uploading to Amazon Glacier...\n')
            for part in filist:
                num = filist.index(part)+1
                #print('\nUploading part {} of {}'.format(filist.index(part)+1, of))
                prop = round(num/of, 3)
                space = round(1-prop)
                graph = "{}{}  {}%".format('|'*round(prop*10000),
                                          ' '*round(space*100),
                                          round(prop*100))

                thash = gl.core.calculate_tree_hash(open(part, 'rb'))    #  tree hash for part calculated
                btrange= totrange + os.path.getsize(part)
                #print('contacting server...')#  upper bound of current filepart defined
                print('{}\r'.format(graph))
                part_response = gl.glacier_client.upload_multipart_part(vaultName=vault,
                                                             uploadId=upid,
                                                             body=open(part, 'rb'),
                                                             checksum=str(thash),  # checksum in string form
                                                             range='bytes {}-{}/*'.format(totrange,
                                                                                          (btrange-1)),
                                                             accountId=acctid)
                response.append(part_response)
                #print('Upload part {} of {} COMPLETE\n'.format(num, of))

                print(graph)
                totrange += size
            full_tree_hash = gl.core.calculate_tree_hash(open(fname, 'rb'))
            completion = gl.glacier_client.complete_multipart_upload(vaultName=vault,
                                                          uploadId=upid,
                                                          archiveSize=str(fullsize),
                                                          checksum=full_tree_hash,
                                                          accountId=acctid)
            gl.trash(gl.PART_PREFIX)
            print('\nMultipart Upload of Archive: {} to Vault {} Completed\n'.format(fname,
                                                                             vault))
            desc.replace('*', '+')  # for safety, * is marker string for history update
            output = {'FileName':fname,
                  'Description':"{}: {} *{}* ".format(completion['ResponseMetadata']['HTTPHeaders']['date'],
                                                desc, fname),
                  'VaultName': vault,
                  'ArchiveId': completion['archiveId'],
                      'Size':str(fullsize)}
            return output
        else:  #  this needs to happen before the success message if
            print('Amazon does not support archive files over 40 GB, {} is {}'.format(fname, gl.size_display(fullsize)))
    except Exception as ex:
        print('Python raised the following excception: {}'.format(ex))
        gl.glacier_client.abort_multipart_upload(vaultName=vault, upload_id=upid)
    finally:
        gl.trash(gl.PART_PREFIX)
        return None

def bar(n):
    chash = ((60*n)//100)
    output = "[{}{}] {}%".format('#' * chash, ' ' * (60-chash), n)
    return output

def mpUpload(fname,
             vault,
             desc,
             acctid,
             hrglass,
             pool=upool):

    '''Initiate a multipart upload, split file into parts, calculate checksums,
    treehash values, for each, and uploads to glacier in parallel according to # of cores in machine.'''

    chunkscheme = gl.chunksize(fname)  # chunkscheme[0]= size of chunk
    fullsize = os.path.getsize(fname)
    size, partcount = chunkscheme[0], chunkscheme[1]
    graph = ''
    print('function started')
    print(size)
    try:
        if size < 42494967296:
            print('size verified')
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
            print('This may take a while, {} is {} watching it happen is not recommended...\n'.format(fname, gl.size_display(fullsize)))
            print('\nsplitting input file into {} parts\n'.format(partcount))
            filist = unxplit(fname, size, gl.PART_PREFIX)
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
                part_params.update({'xthash':thash})
                bytestring.close()
                btrange = totrange + os.path.getsize(part)
                prtrange = (totrange, btrange-1)
                part_params.update({'xrange':prtrange})
                all_params.append(part_params)
                comprop = prop
                prop = round((len(all_params)*100)/of)
                if prop != comprop:
                    print(bar(prop), ' {} of {}'.format(num, of))
                totrange += size
            print('Uploading to Amazon Glacier...\n')
            # map part uploads into parallel scheme using upool
            ccount = 0
            errcount = 0


            for partload in pool.imap_unordered(partfeed, all_params):
                if partload[1] == 0:
                    print('something went wrong with this one')
                    errcount += 1
                else:
                    pdone = done
                    done = round((ccount/of)*100)
                    print('byterange {} successfully uploaded'.format(partload))
                    ccount += 1
                    if done != pdone:
                        print(bar(done))
            print('{} of {} successfully uploaded'.format(ccount, (of)))
            print('{} failed parts'.format(errcount))
            if errcount != 0:
                print('retrying failed parts')

                cuploaded = ccount
                while cuploaded < len(all_params):
                    #  check to see if all parts have uploaded
                    ulcheck=gl.glacier_client.list_parts(vaultName=vault,
                                                     uploadId=upid)
                    uploaded = ulcheck['Parts']
                    cuploaded = len(uploaded)
                    rangit = (i['RangeInBytes'] for i in uploaded)
                    checkparts = (tuple(byterange.split('-')) for byterange in rangit)  # received byteranges from AWS server
                    remains = (left for left in all_params if left['xrange'] not in checkparts)


                    for partload in pool.imap_unordered(partfeed, remains):
                        if partload[1] == 0:
                            print('something went wrong with this one')
                            errcount += 1
                        else:
                            print('bytes {} successfully uploaded!')
                            ccount += 1
                            print(bar(round((ccount/of)*100)))

            # close pool
            pool.close()
            pool.join()  # retry any uploads left over
            full_tree_hash = gl.core.calculate_tree_hash(open(fname, 'rb'))
            completion = gl.glacier_client.complete_multipart_upload(vaultName=vault,
                                                          uploadId=upid,
                                                          archiveSize=str(fullsize),
                                                          checksum=full_tree_hash,
                                                          accountId=acctid)
            gl.trash(gl.PART_PREFIX)
            print('\nMultipart Upload of Archive: {} to Vault {} Completed\n'.format(fname,
                                                                             vault))
              # for safety, * is marker string for history update
            output = {'FileName':fname,
                  'Description':"{}: {} *{}* ".format(completion['ResponseMetadata']['HTTPHeaders']['date'],
                                                desc, fname),
                  'VaultName': vault,
                  'ArchiveId': completion['archiveId'],
                      'Size':str(fullsize)}
            return output
        else:  #  this needs to happen before the success message if
            print('Amazon does not support archive files over 40 GB, {} is {}'.format(fname, gl.size_display(fullsize)))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('Python raised the following excception: {} {} {} {}'.format(exc_type, e, name, exc_tb.tb_lineno))
        gl.glacier_client.abort_multipart_upload(vaultName=vault, upload_id=upid)
    finally:
        gl.trash(gl.PART_PREFIX)
        return None



if os.getcwd() == '/Users/ChanceyGardener/IcePick':
    cheese = sermpUpload('degroot-schervish-2012.pdf','FrontEndTest','Serial benchmark comparison for parallel upload',
                     gl.ACCOUNT_ID,':)')
else:
    mpUpload('LargeTestFile','FrontEndTest',
             'Parallel upload Test Large file', gl.ACCOUNT_ID,':)')
upool.terminate()

#(fname,
#             vault,
#             desc,
#             acctid,
#             hrglass):


def bsplit(in_filename, bytes_per_file, opref, log=False):

    '''Split the input file in_filename into output files of
    bytes_per_file bytes each. Last file may have less bytes.'''

    chunklist = []
    in_fil = open(in_filename, "rb")
    outfil_idx = 1
    out_filename = make_out_filename(opref, outfil_idx)
    out_fil = open(out_filename, "wb")
    implength = os.path.getsize(in_filename)  #  convert to bytestring ^^ then take the length of in_fil
    tfcount, endlength = divmod(implength, bytes_per_file)
    ocount = 0
    if endlength == 0:
        fcount = tfcount
    else:
        fcount = tfcount + 1
    for i in range(fcount):
        #bgen = (i for i in bytes(in_fil[mark:(mark+bytes_per_file)]))
        bgen = read_in_chunks(in_fil, bytes_per_file)
        for i in range(bytes_per_file):
            try:
                out_fil.write(bytes(next(bgen)))
            except StopIteration:
                out_fil.close()
                chunklist.append(out_fil.name)
                ocount += 1
                return chunklist
        out_fil.close()
        chunklist.append(out_fil.name)
        ocount += 1
        outfil_idx += 1
        out_filename = make_out_filename(opref, outfil_idx)
        out_fil = open(out_filename, "wb")


    return chunklist

def sermpUpload(fname,
             vault,
             desc,
             acctid,
             hrglass):

    '''Initiate a multipart upload, split file into parts, calculate checksums,
    treehash values, for each, and uploads to glacier.'''

    chunkscheme = chunksize(fname)  # chunkscheme[0]= size of chunk
    fullsize = getsize(fname)
    size, partcount = chunkscheme[0], chunkscheme[1]
    graph = ''
    try:
        if size < 42494967296:
            try:
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
            filist = unxplit(fname, size, PART_PREFIX)
            of = len(filist)
            if of != partcount:
                print('of doesnt equal partcount...')
            totrange = 0
            response = []
            print('Uploading to Amazon Glacier...\n')
            for part in filist:
                num = filist.index(part)+1
                #print('\nUploading part {} of {}'.format(filist.index(part)+1, of))
                prop = round(num/of, 3)
                space = round(1-prop)
                graph = "{}{}  {}%".format('|'*round(prop*10000),
                                          ' '*round(space*100),
                                          round(prop*100))

                thash = core.calculate_tree_hash(open(part, 'rb'))    #  tree hash for part calculated
                btrange= totrange + getsize(part)
                #print('contacting server...')#  upper bound of current filepart defined
                print('{}\r'.format(graph))
                part_response = glacier_client.upload_multipart_part(vaultName=vault,
                                                             uploadId=upid,
                                                             body=open(part, 'rb'),
                                                             checksum=str(thash),  # checksum in string form
                                                             range='bytes {}-{}/*'.format(totrange,
                                                                                          (btrange-1)),
                                                             accountId=acctid)
                response.append(part_response)
                #print('Upload part {} of {} COMPLETE\n'.format(num, of))

                print(graph)
                totrange += size
            full_tree_hash = core.calculate_tree_hash(open(fname, 'rb'))
            completion = glacier_client.complete_multipart_upload(vaultName=vault,
                                                          uploadId=upid,
                                                          archiveSize=str(fullsize),
                                                          checksum=full_tree_hash,
                                                          accountId=acctid)
            trash(PART_PREFIX)
            print('\nMultipart Upload of Archive: {} to Vault {} Completed\n'.format(fname,
                                                                             vault))
            desc.replace('*', '+')  # for safety, * is marker string for history update
            output = {'FileName':fname,
                  'Description':"{}: {} *{}* ".format(completion['ResponseMetadata']['HTTPHeaders']['date'],
                                                desc, fname),
                  'VaultName': vault,
                  'ArchiveId': completion['archiveId'],
                      'Size':str(fullsize)}
            return output
        else:  #  this needs to happen before the success message if
            print('Amazon does not support archive files over 40 GB, {} is {}'.format(fname, size_display(fullsize)))
    except Exception as ex:
        print('Python raised the following excception: {}'.format(ex))
        glacier_client.abort_multipart_upload(vaultName=vault, upload_id=upid)
    finally:
        trash(PART_PREFIX)
        return None
