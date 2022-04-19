import gym
import gym.spaces
from gym.utils import seeding
from gym.envs.registration import EnvSpec
import enum
import numpy as np
import torch

from . import data

DEFAULT_BARS_COUNT = 10
DEFAULT_COMMISSION_PERC = 0.1


class Actions(enum.Enum):
    Skip = 0
    Buy = 1
    Close = 2


class MarketWatchState:
    def __init__(self, bars_count, commission_perc,
                 reset_on_close, input_shape=tuple(), reward_on_close=True,
                 volumes=True,
                 target_index=0,
                 weights=None
                 ):
        assert isinstance(bars_count, int)
        assert bars_count > 0
        assert isinstance(commission_perc, float)
        assert commission_perc >= 0.0
        assert isinstance(reset_on_close, bool)
        assert isinstance(reward_on_close, bool)
        self.bars_count = bars_count
        self.commission_perc = commission_perc
        self.reset_on_close = reset_on_close
        self.reward_on_close = reward_on_close
        self.volumes = volumes
        self._input_shape = input_shape
        self._target_index = target_index
        self._weights = weights

    def reset(self, prices, offset):
        assert offset >= self.bars_count-1
        self.have_position = False
        self.open_price = 0.0
        self._prices = prices
        self._offset = offset

    @property
    def shape(self):
        # conv2d
        # [batch_size, input_channels, input_height, input_width]
        # [1, NUM_FEAT, NUM_COMP, BARS ]

        num_of_features = self._input_shape[0]
        if self._weights is not None:
            num_of_features += 1
        num_of_companies = self._input_shape[2]
        num_of_periods = self.bars_count
        return [
            num_of_features,
            num_of_periods,
            num_of_companies
        ]

    def encode(self):
        start = self._offset - (self.bars_count-1)
        stop = self._offset+1

        state = [0, 0]
        if self.have_position:
            state = [1, self.open_price]
        state = np.array(state)

        features = self._prices[:, start:stop, :]
        traded_stock_prices = features[0, :, self._target_index].squeeze()

        if self._weights is not None:
            # repate bars_count times weights
            # so as they can be used for convolutions

            weights = np.repeat(
                np.array(self._weights)[np.newaxis, :],
                self.bars_count, axis=0
            )
            # then stack them vertically to
            # the main features
            features = np.vstack([
                features,
                weights[np.newaxis, :, :]
            ])

        return (features, state, traded_stock_prices)

    def _cur_close(self):
        """
        Calculate real close price for the current bar
        """
        # open = self._prices.open[self._offset]
        # rel_close = self._prices.close[self._offset]

        return self._prices[
            0,  # change this to close feature order
            self._offset,
            0  # change this to company offset
        ]

    def step(self, action):
        """
        Perform one step in our price, adjust offset, check for the end of prices
        and handle position change
        :param action:
        :return: reward, done
        """
        # print(action)
        assert isinstance(action, Actions)
        reward = 0.0
        done = False
        close = self._cur_close()
        if action == Actions.Buy and not self.have_position:
            self.have_position = True
            self.open_price = close
            reward -= self.commission_perc
        elif action == Actions.Close and self.have_position:
            reward -= self.commission_perc
            done |= self.reset_on_close
            if self.reward_on_close:
                reward += 100.0 * (close / self.open_price - 1.0)
            self.have_position = False
            self.open_price = 0.0

        self._offset += 1
        prev_close = close
        close = self._cur_close()
        done |= self._offset >= self._prices.shape[1]-1

        if self.have_position and not self.reward_on_close:
            reward += 100.0 * (close / prev_close - 1.0)

        return reward, done


class MarketWatchStocksEnv(gym.Env):
    metadata = {'render.modes': ['human']}
    spec = EnvSpec("MarketWatchStocksEnv-v0")

    def __init__(self, prices, bars_count=DEFAULT_BARS_COUNT,
                 commission=DEFAULT_COMMISSION_PERC,
                 reset_on_close=True,
                 random_ofs_on_reset=True, reward_on_close=False,
                 volumes=False,
                 target_index=0,
                 weights=None
                 ):
        self._prices = prices
        self._state = MarketWatchState(
            bars_count,
            commission,
            reset_on_close,
            reward_on_close=reward_on_close,
            volumes=volumes,
            input_shape=prices.shape,
            target_index=target_index,
            weights=weights

        )

        self.action_space = gym.spaces.Discrete(n=len(Actions))
        self.observation_space = gym.spaces.Box(
            low=-np.inf, high=np.inf,
            shape=self._state.shape, dtype=np.float32)
        self.random_ofs_on_reset = random_ofs_on_reset
        self.seed()

    def reset(self):
        self._instrument = 'TSLA'
        prices = self._prices
        bars = self._state.bars_count
        offset = bars
        # if self.random_ofs_on_reset:
        #     offset = self.np_random.choice(
        #         prices.high.shape[0]-bars*10) + bars
        # else:
        #     offset = bars
        self._state.reset(prices, offset)
        return self._state.encode()

    def step(self, action_idx):
        action = Actions(action_idx)
        reward, done = self._state.step(action)
        obs = self._state.encode()
        info = {
            "instrument": self._instrument,
            "offset": self._state._offset
        }
        return obs, reward, done, info

    def render(self, mode='human', close=False):
        pass

    def close(self):
        pass

    def seed(self, seed=None):
        self.np_random, seed1 = seeding.np_random(seed)
        seed2 = seeding.hash_seed(seed1 + 1) % 2 ** 31
        return [seed1, seed2]
