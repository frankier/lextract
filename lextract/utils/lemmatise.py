from finntk.omor.extract import extract_true_lemmas_span


def fi_lemmatise(x):
    return extract_true_lemmas_span(x.lower(), norm_func=lambda x: x.lower())
