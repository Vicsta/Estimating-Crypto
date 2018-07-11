import numpy as np
import matplotlib.pyplot as plt

def parse(fname):
    pv, qv, tv = [], [], []
    with open(fname) as f :
        for i, line in enumerate(f):
            if i == 0 : continue
            sv = line.split(",")
            if len(sv) < 3 : continue;
            p, q, t = float(sv[0]), float(sv[1]), float(sv[2])
            pv.append(p)
            qv.append(q)
            tv.append(t)
    return pv, qv, tv

class dataSource :
    
    def __init__(self, name, fileName) :
        self.name = name
        self.p, self.q, self.t = parse(fileName)


def plotData(pv, qv, tv):
    dt = np.zeros(len(tv))
    for i in range(1,len(tv)):
        dt[i] = tv[i] - tv[0]
    y = np.array(pv)
    plt.plot(dt, y)
    plt.show()
    


def main():
    xrp = dataSource("xrp", "../data/xrp.csv")
    ltc = dataSource("ltc", "../data/ltc.csv")
    xbt = dataSource("xbt", "../data/xbt.csv")
    eth = dataSource("eth", "../data/eth.csv")
    plotData(eth.p, eth.q, eth.t)
    

if __name__ == '__main__': 
    main()
