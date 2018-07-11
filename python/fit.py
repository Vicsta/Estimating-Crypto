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

def plotData(pv, qv, tv):
    dt = np.zeros(len(tv))
    for i in range(1,len(tv)):
        dt[i] = tv[i] - tv[0]
    y = np.array(pv)
    plt.plot(dt, y)
    plt.show()

class dataSource :
    
    def __init__(self, name, fileName) :
        self.name = name
        self.p, self.q, self.t = parse(fileName)
        self.ix = 0

    def rewind(self):
        self.ix = 0

    def incr(self) :
        self.ix += 1

    def decr(self) :
        self.ix -= 1

    def hasNext(self) :
        return self.ix < len(self.t)

    def nextTime(self):
        return self.t[self.ix]

    def publish(self):
        i = self.ix
        sx = ("%s,%f,%f,%f" % (self.name, self.t[i], self.p[i], self.q[i]))
        self.incr()
        return sx
              

def sourcesHasNext(sources):
    for s in sources:
        if s.hasNext() : return True
    return False

def nextSource(sources):
    assert len(sources) > 0
    next = sources[0]
    for i in range(1,len(sources)):
        if sources[i].nextTime() < next.nextTime():
            next = sources[i]
    return next

def marketSim(sources) :
    while sourcesHasNext(sources):
        next = nextSource(sources)
        print(next.publish())


def main():
    xrp = dataSource("xrp", "../data/xrp.csv")
    ltc = dataSource("ltc", "../data/ltc.csv")
    xbt = dataSource("xbt", "../data/xbt.csv")
    eth = dataSource("eth", "../data/eth.csv")
    marketSim([xrp, ltc, xbt, eth])
#    plotData(eth.p, eth.q, eth.t)
    

if __name__ == '__main__': 
    main()
