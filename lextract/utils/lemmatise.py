from finntk.omor.extract import extract_true_lemmas_span


def fi_lemmatise(x, return_pos=False):
    result = extract_true_lemmas_span(
        x.lower(), norm_func=lambda x: x.lower(), return_pos=return_pos
    )
    if return_pos:
        return {
            k: {
                (upos, tuple(((feat.title(), val.title()) for feat, val in pairs)))
                for (upos, pairs) in v
            }
            for k, v in result.items()
        }
    else:
        return {
            k: {
                tuple(((feat.title(), val.title()) for feat, val in pairs))
                for pairs in v
            }
            for k, v in result.items()
        }
