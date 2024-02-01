import io
import os
import py7zr
import bz2
from rarfile import RarFile
import zipfile, tarfile


Z7_TMP_DIR = './out/z7_temp'
# -----------------------------
# process one zip or tar file
# -----------------------------
def yield_zip_file(file_path, file_type):
    # print ("****", file_path, file_type)
    match file_type:
        case '.csv':
            pass    # not supported
        case '.zip':
            with zipfile.ZipFile(file_path, "r") as zf:
                for fname in zf.namelist():
                    f = zf.open(fname) 
                    yield f, zf.infolist()[0].file_size
            
        case '.gz': # tarfile
            if type(file_path) == str:
                file_name = file_path
                file_obj = None
            else:
                file_name = None
                file_obj = file_path
            with tarfile.open(name  = file_name, fileobj = file_obj, mode = "r:gz") as tar:
                for member in tar.getmembers():
                    f=tar.extractfile(member)
                    # st.write('file size:', member.size)
                    # print (f.name)
                    yield f, member.size
        case '.7z':
            if not os.path.exists(Z7_TMP_DIR):
                os.mkdir(Z7_TMP_DIR)
            with py7zr.SevenZipFile(file_path, mode='r') as zf:
                zf.extractall(Z7_TMP_DIR)
                for fname in os.listdir(Z7_TMP_DIR):
                    file_path = os.path.join(Z7_TMP_DIR, fname)
                    file_size = os.path.getsize()
                    with open(file_path, 'rt', encoding= 'utf8') as f:
                        yield f, file_size
                    os.unlink(file_path)

        case '.bz2':
            if not os.path.exists(Z7_TMP_DIR):
                os.mkdir(Z7_TMP_DIR)
            with bz2.BZ2File(file_path, mode='r') as bz2f:
                data = bz2f.read()              # get the decompressed data
                newfilepath = os.path.join(Z7_TMP_DIR, file_path.name)
                open(newfilepath, 'wb').write(data) # write an uncompressed file
                file_size = os.path.getsize(newfilepath)
                with open(newfilepath, 'rt', encoding= 'utf8') as f:
                    yield f, file_size
                os.unlink(newfilepath)
        case '.rar':     # other
            if type(file_path) == str:
                rar_files = RarFile(file_path)
            else:   # UploadedFile object
                try:
                    rar_files = RarFile(io.BytesIO(file_path.read()))
                except: # home PC does not find le32
                    # LOG_DIR = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Projects\Realestate Reservation Portal\5- Operation\Incident reports\change land-no\DB logs"
                    LOG_DIR = r"C:\Users\yahia\OneDrive - Data and Transaction Services\DTS-data\PortalLogs\DB-Log"
                    rar_files = RarFile(os.path.join(LOG_DIR,file_path.name))

            for f in rar_files.infolist():
                print (f.filename)
                if f.is_dir():
                    print ("directory:", f.filename)
                    continue
                
                try:
                    rar_file = RarFile.open(rar_files, f.filename)
                except Exception as err:    # io.UnsupportedOperation:
                    print (f.filename, f'err opening file: {err}')
                    continue

                yield rar_file, 0   # TODO: change 0 with the file size