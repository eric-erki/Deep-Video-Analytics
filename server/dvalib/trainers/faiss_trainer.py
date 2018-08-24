import hashlib
import logging
import sys

try:
    sys.path.append('/root/thirdparty/faiss/python')
    import faiss
except:
    logging.warning("Could not import faiss")


def train_index(data, index_factory, output_file):
    faiss_index = faiss.index_factory(data.shape[1], str(index_factory))
    faiss_index.train(data)
    faiss.write_index(faiss_index, str(output_file))
    return hashlib.sha1(file(output_file).read()).hexdigest()
