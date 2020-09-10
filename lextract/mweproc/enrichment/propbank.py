from scipy.stats import beta


class Evaluator:
    def __init__(self, hw_cnts, frame_cnts):
        self.hw_cnts = hw_cnts
        self.frame_cnts = frame_cnts
        self.all_props = set(hw_cnts.keys())
        self.frame_props = set(frame_cnts.keys())
        for frame in self.frame_props:
            assert frame in self.all_props

    def survival_at_thresh(self, thresh):
        for prop in self.all_props:
            hw_cnt = self.hw_cnts[prop]
            true_cond_t = self.frame_cnts[prop]
            survival = beta.sf(thresh, hw_cnt + 0.5, hw_cnt - true_cond_t + 0.5)
            yield prop, survival
