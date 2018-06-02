import hashlib
import logging

try:
    import faiss
except ImportError:
    logging.warning("Could not import faiss")


def train_index(data, index_factory, output_file):
    faiss_index = faiss.index_factory(data.shape[0], index_factory)
    faiss_index.add(data)
    faiss_index.train()
    faiss.write_index(faiss_index, output_file)
    return hashlib.sha1(file(output_file).read()).hexdigest()
