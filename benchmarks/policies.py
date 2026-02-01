import numpy as np

class BasePolicy:
    def __init__(self):
        self.name = "Base"

    def predict(self, state, info=None):
        """
        Returns action: [num_executors_idx, mem_idx, tier_idx, comp_idx]
        """
        raise NotImplementedError

class FixedPolicy(BasePolicy):
    """
    Fixed/Static Policy:
    Represents an over-provisioned legacy cluster.
    - Executors: 15 (Constant)
    - Memory: 4GB
    - Tier: WARM (NVMe)
    - Compression: Light
    """
    def __init__(self):
        self.name = "Fixed (Static)"

    def predict(self, state, info=None):
        return np.array([14, 3, 1, 1]), {}

class DexterPolicy(BasePolicy):
    """
    Dexter (Reactive Auto-Scaling):
    Standard Cloud-Native HPA logic.
    - Scales based on CURRENT CPU Utilization.
    - Storage: Fixed to WARM (Standard SSD).
    """
    def __init__(self):
        self.name = "Dexter (Reactive)"
        self.current_executors = 10

    def predict(self, state, info=None):
        cpu_util = state[2]

        if cpu_util > 80:
            self.current_executors += 2
        elif cpu_util < 30:
            self.current_executors -= 1

        self.current_executors = max(0, min(19, self.current_executors))

        tier = 1
        comp = 1
        mem = 3

        return np.array([self.current_executors, mem, tier, comp]), {}

class SeerPolicy(BasePolicy):
    """
    Seer (Predictive Scaling):
    Uses forecasted workload to scale, but lacks storage intelligence.
    - Scales proportional to PREDICTED Load.
    - Storage: Always HOT (Optimizes for Latency, ignores Cost).
    """
    def __init__(self):
        self.name = "Seer (Predictive)"

    def predict(self, state, info=None):
        pred_load = state[-2]

        target_executors = int(pred_load / 50.0)

        valid_exec = max(0, min(19, target_executors))

        tier = 0
        comp = 0
        mem = 3

        return np.array([valid_exec, mem, tier, comp]), {}
