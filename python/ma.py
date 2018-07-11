"""

A variety of moving averages

"""

import numpy as np
import matplotlib.pyplot as plt

def simpleMA(data, period) :
    assert len(data) > period
    result = np.zeros(len(data) - period)
    for i in range(0, len(data) - period) :
        result[i] = np.mean(data[i:i+period])
    return result

def ema(data, period) :
    assert len(data) > period
    a = 2.0/(period + 1.0)
    result = np.zeros(len(data))
    result[0] = data[0]
    for i in range(1, len(data)) :
        result[i] = a*data[i] + (1-a)*result[i-1]
    return result

def main():
    period = 5
    data = np.random.rand(100) * 85.0
    ma = simpleMA(data, period)
    em = ema(data, period)
    x = np.arange(len(data))
    plt.plot(x, data)
    plt.plot(x[:len(ma)], ma)
    plt.plot(x[:len(em)], em)
    plt.show()
    print(np.mean(ma))

if __name__ == '__main__':
    main()

