#!/usr/bin/env python3
# %% tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
import json
from collections import defaultdict
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
from src.models.lib import environ, models, common, validation
upstream = ['combine_fred_yahoo']


# %%


def train_model(
    saves_dir="saves",
    batch_size=32,
    bars_count=10,

    eps_start=1.0,
    eps_final=0.1,
    eps_steps=1000000,

    gamma=0.99,

    replay_size=100000,
    replay_initial=1000,
    reward_steps=2,
    learning_rate=0.0001,
    states_to_evaluate=1000,

    cuda=torch.cuda.is_available(),
    run_name='test',
    ticker=os.environ.get('TRADE_TICKER', 'TSLA')
):
    device = torch.device("cuda" if cuda else "cpu")

    saves_path = pathlib.Path(saves_dir) / f"conv-{run_name}"
    saves_path.mkdir(parents=True, exist_ok=True)

    features = [
        'Close',  # close price should be here since it's used in cur_close method of state
        'High',
        'Low',
        'Open'
    ]

    try:
        # try to fetch data from ploomber build step
        stock_data_path = upstream['combine_fred_yahoo']['data']
        # TODO: weights upstream
        # weights_data_path = upstream['combine_fred_yahoo']['data']
    except Exception:
        # fallbalck to env variables
        stock_data_path = os.environ.get(
            'DATA_PATH', '~/Downloads/fred_yahoo-2.xlsx')
        weights_data_path = os.path.expanduser(os.environ.get(
            'WEIGHTS_PATH', '~/Downloads/tfidf_vals.txt'))

    dfs = pd.read_excel(stock_data_path, sheet_name=features)

    cols = None
    for feature in features:
        if cols is None:
            cols = dfs[feature].columns
        else:
            cols = np.intersect1d(cols, dfs[feature].columns)

    cols = [c for c in cols if 'date' not in c.lower()]
    weights_json = defaultdict(int)
    with open(weights_data_path) as f:
        weights_json.update(json.loads(f.read()))

    weights = [weights_json[c] for c in cols]

    target_stock_index = cols.index(ticker)
    data = np.array([
        dfs[feature][cols] for feature in features
    ]).astype(np.float32)

    env = environ.MarketWatchStocksEnv(
        data, bars_count=bars_count, target_index=target_stock_index, weights=weights)
    env_tst = environ.MarketWatchStocksEnv(
        data, bars_count=bars_count, target_index=target_stock_index, weights=weights)

    env = gym.wrappers.TimeLimit(env, max_episode_steps=1000)
    env_val = env_tst

    net = models.DQNConv1DMarketWatch(
        env.observation_space.shape, env.action_space.n, bars_count).to(device)
    tgt_net = ptan.agent.TargetNet(net)

    selector = ptan.actions.EpsilonGreedyActionSelector(eps_start)
    eps_tracker = ptan.actions.EpsilonTracker(
        selector, eps_start, eps_final, eps_steps)
    agent = ptan.agent.DQNAgent(net, selector, device=device,
                                preprocessor=lambda x: common.state_preprocessor(x, device=device))
    exp_source = ptan.experience.ExperienceSourceFirstLast(
        env, agent, gamma, steps_count=reward_steps)
    buffer = ptan.experience.ExperienceReplayBuffer(
        exp_source, replay_size)
    optimizer = optim.Adam(net.parameters(), lr=learning_rate)

    def process_batch(engine, batch):
        optimizer.zero_grad()
        loss_v = common.calc_loss(
            batch, net, tgt_net.target_model,
            gamma=gamma ** reward_steps, device=device)
        loss_v.backward()
        optimizer.step()
        eps_tracker.frame(engine.state.iteration)

        if getattr(engine.state, "eval_states", None) is None:
            eval_states = buffer.sample(states_to_evaluate)
            eval_states = [np.array(transition.state, copy=False)
                           for transition in eval_states]
            engine.state.eval_states = np.array(eval_states, copy=False)

        return {
            "loss": loss_v.item(),
            "epsilon": selector.epsilon,
        }

    engine = Engine(process_batch)
    tb = common.setup_ignite(engine, exp_source, f"conv-{run_name}",
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
            print(
                f'mean_val ${mean_val}, less than best {engine.state.best_mean_val}')

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
    yield net, agent, data, env

    engine.run(common.batch_generator(buffer, replay_initial, batch_size))

# %%


if __name__ == '__main__':
    runner = train_model(replay_initial=10)
    net, agent, data, env = next(runner)
    print('running the training process')
    next(runner)
# %%
