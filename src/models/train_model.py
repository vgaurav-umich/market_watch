#!/usr/bin/env python3
# %% tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ['combine_fred_yahoo']

# %%
import os
import ptan
import pathlib
import gym.wrappers
import numpy as np
import pandas as pd

import torch
import torch.optim as optim

from ignite.engine import Engine
from ignite.contrib.handlers import tensorboard_logger as tb_logger

from src.models.lib import environ, data, models, common, validation

SAVES_DIR = pathlib.Path("saves")
BATCH_SIZE = 32
BARS_COUNT = 10

EPS_START = 1.0
EPS_FINAL = 0.1
EPS_STEPS = 1000000

GAMMA = 0.99

REPLAY_SIZE = 100000
REPLAY_INITIAL = 1000
REWARD_STEPS = 2
LEARNING_RATE = 0.0001
STATES_TO_EVALUATE = 1000

# %%
cuda = torch.cuda.is_available()
run = 'test'
# args = parser.parse_args()
device = torch.device("cuda" if cuda else "cpu")

saves_path = SAVES_DIR / f"conv-{run}"
saves_path.mkdir(parents=True, exist_ok=True)


features = [
    'High', 'Close', 'Low',
    'Open' # open is broken. has shape (558, 415) vs (692, 403)
    ]

stock_data_path = upstream['combine_fred_yahoo']['data']
dfs = pd.read_excel(stock_data_path, sheet_name=features)

cols = None
for feature in features:
    if cols is None:
        cols = dfs[feature].columns
    else:
        cols = np.intersect1d(cols, dfs[feature].columns)

cols = [c for c in cols if 'date' not in c.lower()]
data = np.array([
    dfs[feature][cols] for feature in features
]).astype(np.float32)

env = environ.MarketWatchStocksEnv(data, bars_count=BARS_COUNT, state_1d=True)
env_tst = environ.MarketWatchStocksEnv(data, bars_count=BARS_COUNT, state_1d=True)


env = gym.wrappers.TimeLimit(env, max_episode_steps=1000)
env_val = env_tst


# %%
net = models.DQNConv1DMarketWatch(env.observation_space.shape, env.action_space.n).to(device)
tgt_net = ptan.agent.TargetNet(net)

selector = ptan.actions.EpsilonGreedyActionSelector(EPS_START)
eps_tracker = ptan.actions.EpsilonTracker(
    selector, EPS_START, EPS_FINAL, EPS_STEPS)
agent = ptan.agent.DQNAgent(net, selector, device=device, preprocessor=lambda x: common.state_preprocessor(x, device=device))
exp_source = ptan.experience.ExperienceSourceFirstLast(
    env, agent, GAMMA, steps_count=REWARD_STEPS)
buffer = ptan.experience.ExperienceReplayBuffer(
    exp_source, REPLAY_SIZE)
optimizer = optim.Adam(net.parameters(), lr=LEARNING_RATE)


def process_batch(engine, batch):
    optimizer.zero_grad()
    loss_v = common.calc_loss(
        batch, net, tgt_net.target_model,
        gamma=GAMMA ** REWARD_STEPS, device=device)
    loss_v.backward()
    optimizer.step()
    eps_tracker.frame(engine.state.iteration)

    if getattr(engine.state, "eval_states", None) is None:
        eval_states = buffer.sample(STATES_TO_EVALUATE)
        eval_states = [np.array(transition.state, copy=False)
                       for transition in eval_states]
        engine.state.eval_states = np.array(eval_states, copy=False)

    return {
        "loss": loss_v.item(),
        "epsilon": selector.epsilon,
    }


engine = Engine(process_batch)
tb = common.setup_ignite(engine, exp_source, f"conv-{run}",
                         extra_metrics=('values_mean',))


@engine.on(ptan.ignite.PeriodEvents.ITERS_10_COMPLETED)
def sync_eval(engine: Engine):
    tgt_net.sync()

    mean_val = common.calc_values_of_states(
        engine.state.eval_states, net, device=device)
    engine.state.metrics["values_mean"] = mean_val
    is_first = False
    if getattr(engine.state, "best_mean_val", None) is None:
        engine.state.best_mean_val = mean_val
        is_first = True

    if engine.state.best_mean_val < mean_val or is_first:
        print("%d: Best mean value updated %.3f -> %.3f" % (
            engine.state.iteration, engine.state.best_mean_val,
            mean_val))
        path = saves_path / ("mean_value_%.3f.data" % mean_val)
        torch.save(net.state_dict(), path)
        engine.state.best_mean_val = mean_val
    else:
        print(f'mean_val ${mean_val}, less than best {engine.state.best_mean_val}')

def validate(engine: Engine):
    res = validation.validation_run(env_tst, net, device=device)
    print("%d: tst: %s" % (engine.state.iteration, res))
    for key, val in res.items():
        engine.state.metrics[key + "_tst"] = val
    res = validation.validation_run(env_val, net, device=device)
    print("%d: val: %s" % (engine.state.iteration, res))
    for key, val in res.items():
        engine.state.metrics[key + "_val"] = val
    val_reward = res['episode_reward']
    if getattr(engine.state, "best_val_reward", None) is None:
        engine.state.best_val_reward = val_reward
    if engine.state.best_val_reward < val_reward:
        print("Best validation reward updated: %.3f -> %.3f, model saved" % (
            engine.state.best_val_reward, val_reward
        ))
        engine.state.best_val_reward = val_reward
        path = saves_path / ("val_reward-%.3f.data" % val_reward)
        torch.save(net.state_dict(), path)


event = ptan.ignite.PeriodEvents.ITERS_100_COMPLETED
tst_metrics = [m + "_tst" for m in validation.METRICS]
tst_handler = tb_logger.OutputHandler(
    tag="test", metric_names=tst_metrics)
tb.attach(engine, log_handler=tst_handler, event_name=event)

val_metrics = [m + "_val" for m in validation.METRICS]
val_handler = tb_logger.OutputHandler(
    tag="validation", metric_names=val_metrics)
tb.attach(engine, log_handler=val_handler, event_name=event)

engine.run(common.batch_generator(buffer, REPLAY_INITIAL, BATCH_SIZE))

# %%
