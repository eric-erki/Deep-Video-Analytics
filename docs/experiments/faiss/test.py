import sys, os
sys.path.append('/root/DVA/server/')
sys.path.append('/root/thirdparty/faiss/python/')
import dvalib
import faiss
import numpy as np
from dvalib import approximator
from dvalib import retriever
import django
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
from dvaapp.operations import approximation
from dvaapp import models
di = models.IndexEntries.objects.get(approximator_shasum="8d02b70ec4749b925ec54cd0e360c54812362554")
mat, entries = di.load_index()
print mat
index = faiss.read_index(str(mat).replace('//','/'),faiss.IO_FLAG_MMAP)
a, da = approximation.Approximators.get_approximator_by_shasum("8d02b70ec4749b925ec54cd0e360c54812362554")
other_index = faiss.read_index(a.index_path)
print other_index.ntotal
print index.ntotal
other_index.merge_from(index,0)
