import sys
import os
import numpy as np
from stable_baselines3 import TD3
from stable_baselines3.common.noise import NormalActionNoise

sys.path.append(os.path.join(os.path.dirname(__file__), '__init__.py'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from optimization.meta_controller.meta_environment import MetaSparkEnv

def create_model():
    print(" Creating Dummy Meta-Controller Model (Verification Mode)...")

    env = MetaSparkEnv(steps_per_meta_step=10)

    n_actions = env.action_space.shape[-1]
    action_noise = NormalActionNoise(mean=np.zeros(n_actions), sigma=0.1 * np.ones(n_actions))

    model = TD3(
        "MlpPolicy",
        env,
        action_noise=action_noise,
        verbose=1
    )

    os.makedirs("data/models/meta_controller", exist_ok=True)
    save_path = "data/models/meta_controller/td3_meta_agent"
    model.save(save_path)
    print(f" Model saved to {save_path}")

if __name__ == "__main__":
    create_model()
