import tarfile
from glob import glob
import os
from os.path import join as _join
from os.path import split as _split
import shutil

SCRATCH = '/media/ramdisk'

def tar_gz_is_valid(tar_fn):
    
    scn_path = _join(SCRATCH, _split(tar_fn)[-1].replace('.tar.gz', ''))
    is_valid = False
    try:
        tar = tarfile.open(tar_fn)
        tar.extractall(path=scn_path)
        tar.close()
        is_valid = True
    except:
        pass

    try:
        shutil.rmtree(scn_path)
    except:
        pass

    return is_valid
        
if __name__ == "__main__":
    fp = open('not_valid.txt', 'w')

    tar_fns = glob('/geodata/torch-landsat/*.tar.gz')
    for fn in tar_fns:
        print(fn)
        is_valid = tar_gz_is_valid(fn)
        print(is_valid)
        if not is_valid:
            fp.write(fn + '\n')
            os.remove(fn)

    fp.close()
       
