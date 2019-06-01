# /usr/bin/python3
# Icepyck interface for Amazon Glacier.

from time import time
zero = time()
print('loading libraries...\n')
import glacier_uploader as gl
from os import path, getcwd
import multiprocessing as mp

one=time()
print('took {} seconds to load libraries'.format(gl.timer(zero, one)))


hist = gl.get_history('history.p') #  returns dict containing histtory
retrhist = gl.get_history('retrhist.p')
job_info = ((job['jobId'], gl.vaultextract(job['location'])) for job in retrhist)
all_jobs = []
for jobid, vault in job_info:
    try:
        job = gl.glacier.Job(account_id=gl.ACCOUNT_ID,
                             id=jobid,
                             vault_name=vault)
        all_jobs.append(job)
    except Exception as ex:
        print(ex)


# creates a vector of boto3 Job objects
#all_jobs = (gl.glacier.Job(account_id=gl.ACCOUNT_ID, id=jobid, vault_name=vault) for jobid, vault in job_info)
#active_jobs = [job for job in all_jobs if not job.completed]
active_jobs = []
for j in all_jobs:
    try:
        j.DescribeJob()
        active_jobs.append(j)
    except:
        pass

for i in active_jobs:
    gl.rpool.apply_async()

# creates a vector of current vault names
current_vaults = [i.lower() for i in gl.list_vaults(hist)]


cwd = getcwd()

# kBjDmfzMSisDDGbEeSyVr0vYDoxD-efsAa4wqr2DoQlJYr2NcdvrGH6Uf5Q6GkzXDWugpZekp4Z_bVljyJVA4ZFUFbM5

waiting = """\n_¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶
_¶¶___________________________________¶¶
_¶¶___________________________________¶¶
__¶¶_________________________________¶¶_
__¶¶_________________________________¶¶_
___¶¶_______________________________¶¶__
___¶¶______________________________¶¶___
____¶¶¶__________________________¶¶¶____
_____¶¶¶¶_¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶_¶¶¶¶_____
_______¶¶¶¶_¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶_¶¶¶¶_______
_________¶¶¶¶_¶¶¶¶¶¶¶¶¶¶¶¶_¶¶¶¶_________
___________¶¶¶¶¶_¶¶¶¶¶¶¶_¶¶¶¶___________
______________¶¶¶¶_¶¶¶_¶¶¶______________
________________¶¶¶_¶_¶¶________________
_________________¶¶¶_¶¶_________________
__________________¶¶_¶¶_________________
__________________¶¶_¶__________________
__________________¶¶_¶¶_________________
________________¶¶¶_¶_¶¶¶_______________
_____________¶¶¶¶¶__¶__¶¶¶¶¶____________
__________¶¶¶¶¶_____¶_____¶¶¶¶__________
________¶¶¶¶________¶_______¶¶¶¶¶_______
_______¶¶¶__________¶__________¶¶¶¶_____
_____¶¶¶____________¶____________¶¶¶____
____¶¶¶_____________¶______________¶¶___
___¶¶¶______________¶_______________¶¶__
___¶¶_______________¶________________¶¶_
__¶¶________________¶________________¶¶_
__¶¶_______________¶¶¶________________¶_
__¶¶_¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶_¶¶
__¶¶_¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶_¶¶
__¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶¶\n"""

pyck = """


88                       88888888ba                         88
88                       88      "8b                        88
88                       88      ,8P                        88
88  ,adPPYba,  ,adPPYba, 88aaaaaa8P' 8b       d8  ,adPPYba, 88   ,d8
88 a8"     "" a8P_____88 88          `8b     d8' a8"     "" 88 ,a8"
88 8b,        8PP        88           `8b   d8'  8b         8888[
88 "8a,   ,aa "8b,   ,aa 88            `8b,d8'   "8a,   ,aa 88`"Yba,
88  `"Ybbd8"'  `"Ybbd8"' 88              Y88'     `"Ybbd8"' 88   `Y8a
                                         d8'
                                        d8'
                                        """
print(pyck)
print("\n\nWelcome to IcePick, a simple python based command line interface for Amazon Glacier\n",
      'If you have not, move any files you wish to upload into:\n\n {}\n'.format(cwd),
      "\nenter a known command or enter 'help' for a list of supported operations\n")

def yesno(request, error=''):
    '''non case sensitive yes or no request macro
    first argument is request text, second is error message (default is empty string)
    returns Boolean'''
    usr = input(request)
    while usr.lower() not in ['y','yes','no','n']:
        usr = input('input not understood {}: '.format(error))
    if usr.lower()[0] == 'y':
        return True
    else:
        return False

def upload():
    vaultname = input('enter vault name: ')
    while vaultname.lower() not in current_vaults:
        if vaultname.lower() == 'cancel':
            return None
        vaultname = input('Vault name not found, enter vault name or \n'
                      "create a new vault using the command 'new_vault': ")
        if vaultname == 'new_vault':
            newname = input('enter name for new vault: ')
            gl.new_vault(newname)
            vaultname = newname
            break
    filename = input('enter file path: ')
    if filename.lower() == 'cancel':
        return None
    while not path.isfile(filename):
        if filename.lower() == 'cancel':
            return None
        else:
            filename = input("file not found; enter full file path from home directory,"
                             " e.g.) '{}' enter file path or 'cancel':\n".format(cwd))
            if filename.lower() == 'cancel':
                return None

    desc_input = input('Enter a brief description of this archive: ')
    desc = '{} '.format(desc_input)
    confirm = "CONFIRM:\nUpload {} to vault: {} with description\n'{}'\n(y)es/(n)o: ".format(filename,
                                                                                             vaultname,
                                                                                             desc)

    ULconfirm= yesno(confirm,
                     '(Y)es to upload archive or (N)o to cancel')
    if ULconfirm is True:
        upresponse = gl.mpUpload(fname=filename,
                                 acctid=gl.ACCOUNT_ID,
                                 vault=vaultname,
                                 desc=desc,
                                 hrglass=waiting)
        if upresponse is None:
            print('An Error occurred, make sure vault name (case sensitive) is correct')
            if yesno('Try another upload?') is True:
                upload()
            else:
                return None
        else:
            hist.append(upresponse) #  history organized with archive names as keys and response metadata dicts as values
    else:
        again = yesno('Upload Cancelled, try another upload? (Y)es / (N)o: ')
        if again is True:
            upload()
        else:
            return None


#TODO: make a command line session class

def command(resp):
    # initiates a command line session
    if isinstance(resp, dict) is False:
        raise TypeError('resp must be a dictionary with function objects as values')
    uput = input('enter command: ')
    termcode = 'terminate_session'
    while uput not in resp.keys():
        print("input not understood, enter 'help' for a list of available commands")
        uput = input('enter command: ')
    if uput == termcode:
            return None
    else:
        try:
            resp[uput]()
        except Exception:
            with Exception as e:
                print('Python raised the following exception: {}'.format(e))
        finally:
            command(resp)
        return None

def request_archive_retrieval(joblist=all_jobs):
    pool = gl.mp.Pool()
    list_vaults()
    fhist = hist  # exit the function if this list is empty; suggest request by id method in error message
    email = input('enter a valid email address for notification of completion: ')
    phone = input('enter a phone number (with country code) for an sms capable device: ')
    filename = input('enter file name as logged in history for the archive you wish to retreive: ')
    if filename.lower() == 'cancel':
            output = None
            return output
    duplist = []
    for log in fhist:
        if log['FileName'] == filename:
            duplist.append(log)
    dup = len(duplist)

    if dup is not 1:  # if specification by filename is unclear
        if dup > 1:  # if there are more than one files by the given name
            print('{} entries found for filename {}'.format(dup, filename))
            for entry in range(dup):  # all possible duplicates iterated over by index
                file = duplist[entry]  # entry is the list index of an entry in history
                count = entry + 1
                print("""
                {}. {}
                    -Vault: {}
                    -Archive ID: {}
                """.format(count, file['Description'], file['VaultName'], file['ArchiveId']))
            choice = input('enter the list index (as shown above) for the archive you would like to retrieve')
            if choice.lower() == 'cancel':
                output = None
                return output
            while isinstance(choice, str) is True:
                try:
                    while int(choice) > dup+1:
                        choice = input('List index out of range ({})'.format(count))
                    choice = int(choice)

                except ValueError:
                    choice = input('input not understood, enter a number between 1 and {} corresponding to the'
                        'archive you would like to retrieve: '.format(count))

            output = gl.retrieval_request(vault=duplist[choice-1]['VaultName'],  #  indexes should be corresponding to choice - 1 (entry)
                                 retrlog=retrhist,    #  retrieval history is updated on back end
                                 arcid=duplist[choice-1]['ArchiveId'],
                                 email=email,
                                 phone=phone,
                                 arc=True)
            joblist.append(output[1])
            req = pool.apply_async(gl.retrieve(job=output[1], arc=True, fname=filename))
            req.get()
            pool.close()
            pool.join()


        elif dup == 0:
            print("\nfilename {} not found in upload history, enter command 'show_vaults' to display vault history\n"
                  "or enter 'idretrieve' to request a retrieval for an archive from the archive's ID code directly.\n".format(filename))
            output = None
    else:   #  if only one file matches file name
        choice = duplist[0]
        try:
            output = gl.retrieval_request(vault=choice['VaultName'],
                                          retrlog=retrhist,
                                          arcid=choice['ArchiveId'],
                                          email=email,
                                          phone=phone,
                                          arc=True)
        except Exception as e:
            exc_type, exc_obj, exc_tb = gl.sys.exc_info()
            name = gl.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('Python raised the following excception: {} {} {} {}'.format(exc_type, e, name, exc_tb.tb_lineno))
            output = None

    return output

def inventory_request(email=None,
                      phone=None):
    '''initiates an inventory retrieval job and corresponding SNS topic '''

    #while email is None and phone is None:
    email = input('enter a valid email address for notification of completion: ')
    if email == '':
        email = None
    phone = input('enter a phone number (with country code) for an sms capable device: ')
    if phone == '':
        phone = None
    vault = input('Vault name for inventory request: ')
    while vault.lower() not in current_vaults:
        print("Vault not found")
        vault = input("enter existing vault name or enter 'show_vaults' to display current vaults: ")
        if vault == 'show_vaults':
            gl.list_vaults(hist,
                           display=True)
    doc = gl.retrieval_request(vault=vault,
                           retrlog=retrhist,
                               email=email,
                               phone=phone)
    jobid = doc['jobId']

    root, retriever = mp.Pipe()  # retriever runs gl.retrieve and waits for glacier job to complete

# retrieval_process is the child process running retrieve function.

    retrieval_process = mp.Process(target=gl.retrieve,
                                   args=(jobid,
                                         vault),
                                   kwargs=retriever)
    retrieval_process.start()
    output = root.rcv()
    print(output)
    retrieval_process.join()




    return doc

def update_history(infilename, history=hist):
    try:
        dcode = gl.inventory_reader(open(infilename, 'rb'))
        newdat = gl.reporter(dcode, infilename, history)

    except Exception:
        with Exception as e:
            print('\n{}\n'.format(e))



def job_id_retrieve():
    '''object is either relevant archive file name as listed in history for archive retrieval, or
    relevant vault name for inventory retrieval'''

    jobid=input('Enter job id: ')
    object=input('Enter either filename or Vault Name: ')
    try:
        gl.retrieve(jobid, object)

    except Exception:
        with Exception as e:
            print(e)

    return None

def list_vaults():
    gl.list_vaults(hist, display=True)

def end():
    return 'terminate_session'

def delete_vault():
    '''deletes an existing vault'''
    global current_vaults
    name = input('enter the name of an empty existing vault to delete: ')
    while name not in current_vaults:  # name.lower()
        name = input('Vault not found, enter the name of an existing empty vault: ')
    if yesno('Confirm: delete {}, (Y)es/(N)o'.format(name)) is True:
        try:
            gl.delete_vault(name)
            print('{} deleted successfully'.format(name))
        except:
            print('Something went wrong, remember that Amazon Web Services only allows for the deletion of empty vaults: ')
        current_vaults = [i.lower() for i in gl.list_vaults(hist)]
        return None
    else:
        print('Cancelled, {} was not deleted'.format(name))
        return None

def cleanup():
    gl.trash('part_')



def idretrieve():

    '''makes archive retrieval request directly from archiveid. Simple, low-level'''

    vault=input('Enter Vault Name: ')
    archiveid=input('Enter Archive ID code: ')
    try:
        response = gl.retrieval_request(vault=vault,
                             arcid=archiveid,
                             arc=True,
                             retrlog=retrhist)

        print('checking for vault retrieval status')
        gl.retrieve(jobid=response['jobId'],
                    vault=vault)
        return response
    except:
        print('Something went wrong. Double-check your archiveid')


def show_help():
    print("""


    IcePick Commands:

    'show_vaults' -------------- prints a list of current vaults and archives in Glacier
    'upload' ------------------- begins archive upload process to a new or existing vault
    'terminate_session'--------- ends current session
    'request_archive_retrieval-- initiates an archive retrieval job, sending notification upon completion to a valid
                                 email address, sms-enabled mobile device, or both
    'inventory_request'--------- makes a vault inventory request with notification of completion similar to that
                                 for archive requests
    'delete_vault'-------------- deletes an existing vault. Only works on Vaults that are empty.
    'jobid_retrieve'------------ retrieve job output from jobid code
    'archiveid_retrieval'------- request archive retrieval from AWS archive id (available in show_vaults)""")
    print('\n')

funkbank={'show_vaults':list_vaults,
          'upload':upload,
          'help':show_help,
          'terminate_session':None,
          'request_archive_retrieval':request_archive_retrieval,
          'delete_vault':delete_vault,
          'inventory_request':inventory_request,
          'archiveid_retrieval':idretrieve,
          'cleanup':cleanup,
          'jobid_retrieve':job_id_retrieve}

if __name__ == "__main__":
    command(funkbank)



    gl.save_history(hist,
                    'history.p')
    gl.save_history(retrhist,
                    'retrhist.p')
    print(hist)
    print(retrhist)
