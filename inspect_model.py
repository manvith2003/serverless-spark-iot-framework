from stable_baselines3 import PPO

# Load your best trained PPO model
model = PPO.load("data/models/ppo_resource_allocator/best_model/best_model.zip")

# Print policy structure
print(model.policy)
