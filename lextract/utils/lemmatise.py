from finntk.omor.extract import extract_true_lemmas_span


def fi_lemmatise(x):
    result = extract_true_lemmas_span(x.lower(), norm_func=lambda x: x.lower())
    return {
        k: {tuple(((feat.title(), val.title()) for feat, val in pairs)) for pairs in v}
        for k, v in result.items()
    }