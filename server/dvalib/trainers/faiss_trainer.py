import hashlib
import logging

try:
    import faiss
except ImportError:
    logging.warning("Could not import faiss")


def train_index(data, index_factory, output_file):
    faiss_index = faiss.index_factory(data.shape[1], index_factory)
    faiss_index.train(data)
    faiss.write_index(faiss_index, output_file)
    return hashlib.sha1(file(output_file).read()).hexdigest()
