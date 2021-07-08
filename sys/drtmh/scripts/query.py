import pickle
import collections
import itertools

from runner import RoccRunner
from runner import gen_key

def q_gen_key(params):
    k = gen_key(params) + params["config"]
    return k

def print_entry(res):
    print("%f %f %f %f %f params " \
          % (float(res["thr"]),float(res["medium-lat"]),\
             float(res["50-lat"]),float(res["90-lat"]),float(res["avg-lat"])))

## calculate the avg of results
def get_avg(l):
    if len(l) ==  0:
        return
    a = l[0]
    res = {}

    for i in l:
#        print_entry(i)
        pass

    for key in a:
        sum = 0.0
        temp = []
        for i in l:
            temp.append(i)
        temp.sort()
        ## elimiate some indexes
        start_idx = min(0,len(temp) - 1)
        end_idx   = max(0,len(temp) - 1)
        size = end_idx - start_idx + 1
        while start_idx <= end_idx:
            sum += temp[start_idx][key]
            start_idx += 1
            pass
        res[key] = sum / size
    return res

class RoccQuerier(object):
    def __init__(self,cache_file = "trace"):
        load_results = {}
        try:
            f = open(cache_file)
            load_results = pickle.load(f)
            f.close()
        except:
            pass
        self.caches = {}
        self.results = {}
        sets = {} ## used to remove duplicates

        ## fill in the entries
        for e in load_results:
            p = e["params"]
            n_key = q_gen_key(p)
            if self.results.has_key(n_key):
                self.results[n_key].append(e["res"])
            else:
                self.results[n_key] = [e["res"]]
            for k in p:
                if self.caches.has_key(k):
                    if p[k] in sets[k]:
                        pass
                    else:
                        self.caches[k].append(p[k])
                        sets[k].add(p[k])
                else:
                    sets[k] = set()
                    sets[k].add(p[k])
                    self.caches[k] = [p[k]]
        print(self.results,len(self.results))

    def query(self,q = {}):
        query = {"exe":[], "bench":[], "config":[],\
                 "m":[],"t":[],"c":[],"r":[]}

        for k in q:
            assert query.has_key(k)
            if isinstance(q[k], list):
                for e in q[k]:
                    query[k].append(e)
            else:
                query[k].append(q[k]) ## single entries

        ## fill in the blank
        for k in query:
            if len(query[k]) == 0:
                query[k] = self.caches[k]
        print(query)
        ## read the results

        for l in itertools.product(query["bench"],query["config"],query["exe"]):
            bench,config,exe = l
            for j in itertools.product(query["m"],query["t"],query["c"],query["r"]):
                m,t,c,r = j
                key = {"bench":bench,"config":config,"exe":exe,\
                       "m":m,"t":t,"c":c,"r":r}
                assert self.results.has_key(q_gen_key(key))
                res = self.results[q_gen_key(key)]

                res = get_avg(res)
                try:
                    print("%f %f %f %f %f params %s" \
                          % (float(res["thr"]),float(res["medium-lat"]),\
                             float(res["50-lat"]),float(res["90-lat"]),float(res["avg-lat"]),str(key)))
                except:
                    print("error",key)
            print("") ## a dummy line for seperation
