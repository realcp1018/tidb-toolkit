# coding=utf-8
# @Time: 2022/1/7 14:56
# @Author: forevermessi@foxmail.com
"""
从pd http api中获取store,region信息，以便进行快速统计、过滤、排序等操作
"""
import argparse
import json
import requests
from utils.logger import StreamLogger
from time import sleep
from pprint import pprint

# const
All_Support_Actions = ["showStore", "showStores", "showRegion", "showRegions", "showStoreRegions", "removeRegionPeer",
                       "removeStorePeers"]
# TODO: 4 new actions -> showReionsWith[N]Peer where 1<=N<=4
# TODO: 2 new actions -> safeRemoveRegionPeer, safeRemoveStorePeers
#                        remove peer/peers when len(Region peers on Up stores after removed)>2

# logger
logger = StreamLogger()


# arg parse
def argParse():
    parser = argparse.ArgumentParser(description="PD HTTP API store/region info formatter.")
    parser.add_argument("-u", dest="url", required=True, help="PD Addr(ip:port)")
    parser.add_argument("-s", "--store_id", dest="storeID", type=int, help="Store ID")
    parser.add_argument("-r", "--region_id", dest="regionID", type=int, help="Region ID")
    parser.add_argument("-o", "--option", dest="option", required=True, choices=All_Support_Actions,
                        help="Store/Region Actions")
    parser.add_argument("-l", "--limit", dest="limit", type=int, default=5, help="Region show limit(default 5)")
    parser.add_argument("-t", "--interval-time", dest="interval", type=int, default=3,
                        help="Operator create interval time")
    return parser.parse_args()


class OptionHandler(object):
    def __init__(self, url, storeID, regionID, option, limit, interval):
        self.__url = url
        self.__storeID = storeID
        self.__regionID = regionID
        self.__option = option
        self.__limit = limit
        self.__interval = interval

    # 展示单个store信息
    def showStore(self):
        if not self.__storeID:
            print("Store ID should be specified!")
            exit(1)
        store = Store.from_api_storeid(pd_addr=self.__url, store_id=self.__storeID)
        # Output Demo:
        # Glossary: LCt->LeaderCount  RCt->RegionCount  LWt:->LeaderWeight  RWt:->RegionWeight
        # StoreID   StoreAddr           State   LCt/RCt         LWt/RWt   StartTime
        # -------   ---------           -----   -------         -------   ---------
        print("%-15s%-30s%-15s%-15s%-15s%-30s" % ("StoreID", "StoreAddr", "State", "LCt/RCt", "LWt/RWt",
                                                  "StartTime"))
        print("%-15s%-30s%-15s%-15s%-15s%-30s" % ("-------", "---------", "-----", "-------", "-------",
                                                  "---------"))
        print("%-15s%-30s%-15s%-15s%-15s%-30s" % (store.store_id, store.address, store.state_name,
                                                  "%s/%s" % (store.leader_count, store.region_count),
                                                  "%s/%s" % (store.leader_weight, store.region_weight),
                                                  store.start_ts))

    # 展示所有store信息
    def showStores(self):
        stores = Store.from_api_all(pd_addr=self.__url)
        print("%-30s%-15s%-15s%-15s%-15s%-30s" % ("StoreAddr", "StoreID", "State", "LCt/RCt", "LWt/RWt",
                                                  "StartTime"))
        print("%-30s%-15s%-15s%-15s%-15s%-30s" % ("---------", "-------", "-----", "-------", "-------",
                                                  "---------"))
        for store in sorted(stores, key=lambda s: s.address):
            print("%-30s%-15s%-15s%-15s%-15s%-30s" % (store.address, store.store_id, store.state_name,
                                                      "%s/%s" % (store.leader_count, store.region_count),
                                                      "%s/%s" % (store.leader_weight, store.region_weight),
                                                      store.start_ts))

    # 展示单个regions信息
    def showRegion(self):
        if not self.__regionID:
            print("Region ID should be specified!")
            exit(1)
        region = Region.from_api_regionid(pd_addr=self.__url, region_id=self.__regionID)
        # Output Demo:
        # RegionID  StoreList       Leader   LeaderAddr          DownPeersStoreID   PendingPeersStoreID    Size   Keys
        # --------  ---------       ------   ----------          ----------------   -------------------    ----   ----
        print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % ("RegionID", "StoreList", "Leader",
                                                            "LeaderAddr", "DownPeersStoreID",
                                                            "PendingPeersStoreID", "Size", "Keys"))
        print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % ("--------", "---------", "------",
                                                            "----------", "----------------",
                                                            "-------------------", "----", "----"))
        storeList = [p["store_id"] for p in region.peers]
        leader = region.leader["store_id"]
        leaderAddr = Store.from_api_storeid(pd_addr=self.__url, store_id=leader).address
        if region.down_peers:
            downPeersStoreID = [p["peer"]["store_id"] for p in region.down_peers]
        else:
            downPeersStoreID = []
        if region.pending_peers:
            PendingPeersStoreID = [p["store_id"] for p in region.pending_peers]
        else:
            PendingPeersStoreID = []
        print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % (region.region_id, storeList, leader,
                                                            leaderAddr, downPeersStoreID,
                                                            PendingPeersStoreID, region.approximate_size,
                                                            region.approximate_keys))

    # 展示所有regions信息(默认输出前self.limit个)
    def showRegions(self):
        regions = Region.from_api_all_limit(pd_addr=self.__url, limit=self.__limit)
        # Output Demo:
        # RegionID  StoreList       Leader   LeaderAddr          DownPeersStoreID   PendingPeersStoreID    Size   Keys
        # --------  ---------       ------   ----------          ----------------   -------------------    ----   ----
        print("# topRead {0} Regions(limit {0}):".format(self.__limit))
        print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % ("RegionID", "StoreList", "Leader",
                                                            "LeaderAddr", "DownPeersStoreID",
                                                            "PendingPeersStoreID", "Size", "Keys"))
        print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % ("--------", "---------", "------",
                                                            "----------", "----------------",
                                                            "-------------------", "----", "----"))
        for region in regions:
            storeList = [p["store_id"] for p in region.peers]
            leader = region.leader["store_id"]
            leaderAddr = Store.from_api_storeid(pd_addr=self.__url, store_id=leader).address
            if region.down_peers:
                downPeersStoreID = [p["peer"]["store_id"] for p in region.down_peers]
            else:
                downPeersStoreID = []
            if region.pending_peers:
                PendingPeersStoreID = [p["store_id"] for p in region.pending_peers]
            else:
                PendingPeersStoreID = []
            print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % (region.region_id, storeList, leader,
                                                                leaderAddr, downPeersStoreID,
                                                                PendingPeersStoreID,
                                                                region.approximate_size,
                                                                region.approximate_keys))

    # 展示某个storeID上的所有regions信息(默认输出前self.limit个)
    def showStoreRegions(self):
        if not self.__storeID:
            print("Store ID should be specified!")
            exit(1)
        regions = Region.from_api_storeid(pd_addr=self.__url, store_id=self.__storeID)
        # Output Demo:
        # RegionID  StoreList       Leader   LeaderAddr          DownPeersStoreID   PendingPeersStoreID    Size   Keys
        # --------  ---------       ------   ----------          ----------------   -------------------    ----   ----
        print("# top {0} Regions(limit {0}):".format(self.__limit))
        print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % ("RegionID", "StoreList", "Leader",
                                                            "LeaderAddr", "DownPeersStoreID",
                                                            "PendingPeersStoreID", "Size", "Keys"))
        print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % ("--------", "---------", "------",
                                                            "----------", "----------------",
                                                            "-------------------", "----", "----"))
        for region in regions[:self.__limit]:
            storeList = [p["store_id"] for p in region.peers]
            leader = region.leader["store_id"]
            leaderAddr = Store.from_api_storeid(pd_addr=self.__url, store_id=leader).address
            if region.down_peers:
                downPeersStoreID = [p["peer"]["store_id"] for p in region.down_peers]
            else:
                downPeersStoreID = []
            if region.pending_peers:
                PendingPeersStoreID = [p["store_id"] for p in region.pending_peers]
            else:
                PendingPeersStoreID = []
            print("%-15s%-40s%-15s%-30s%-25s%-25s%-10s%-10s" % (region.region_id, storeList, leader,
                                                                leaderAddr, downPeersStoreID,
                                                                PendingPeersStoreID,
                                                                region.approximate_size,
                                                                region.approximate_keys))

    # 移除指定self.regionID在self.storeID上的peer(副本)，一般用于删除异常副本，之后由tidb集群的raft机制自动补全3副本
    def removeRegionPeer(self):
        if not self.__storeID or not self.__regionID:
            print("Both Store ID and Region ID should be specified!")
            exit(1)
        region = Region.from_api_regionid(pd_addr=self.__url, region_id=self.__regionID)
        isSuccess, respText = region.remove_peer(pd_addr=self.__url, store_id=self.__storeID)
        if isSuccess:
            logger.info("Operator remove-peer created for region %d's peer on store %d[1/1]." % (self.__regionID,
                                                                                                 self.__storeID))
        else:
            logger.error(respText)

    # 移除指定self.storeID上的所有peer(副本)，一般用于删除异常store上的副本，之后由tidb集群的raft机制自动补全3副本
    def removeStorePeers(self):
        if not self.__storeID:
            print("Store ID should be specified!")
            exit(1)
        storeRegions = Region.from_api_storeid(pd_addr=self.__url, store_id=self.__storeID)
        i = 0
        region_count = len(storeRegions)
        while i < region_count:
            region = storeRegions[i]
            isSuccess, respText = region.remove_peer(pd_addr=self.__url, store_id=self.__storeID)
            if isSuccess:
                logger.info("Operator remove-peer created for region %-10d peer on store %d[%d/%d]."
                            % (region.region_id,
                               self.__storeID,
                               i + 1,
                               region_count
                               ))
            else:
                logger.error("Operator remove-peer created failed for region %-10d peer on store %d[%d/%d].\n%s"
                             % (region.region_id, self.__storeID, i + 1, region_count, respText))
            sleep(self.__interval)
            i += 1

    # 根据给定的option执行对应的指令
    def run(self):
        if self.__option == "showStore":
            self.showStore()
        elif self.__option == "showStores":
            self.showStores()
        elif self.__option == "showRegion":
            self.showRegion()
        elif self.__option == "showRegions":
            self.showRegions()
        elif self.__option == "showStoreRegions":
            self.showStoreRegions()
        elif self.__option == "removeRegionPeer":
            self.removeRegionPeer()
        elif self.__option == "removeStorePeers":
            self.removeStorePeers()
        else:
            pass


class Store(object):
    def __init__(self, store_id=None, address=None, labels=None, last_heartbeat=None, last_heartbeat_ts=None,
                 version=None, status_address=None, git_hash=None, deploy_path=None, state_name=None,
                 start_timestamp=None, start_ts=None, capacity=None, available=None, used_size=None, leader_count=None,
                 leader_size=None, leader_score=None, leader_weight=None, region_count=None, region_size=None,
                 region_score=None, region_weight=None, uptime=None):
        self.store_id = store_id
        self.address = address
        self.labels = labels
        self.last_heartbeat = last_heartbeat
        self.last_heartbeat_ts = last_heartbeat_ts
        self.version = version
        self.status_address = status_address
        self.git_hash = git_hash
        self.deploy_path = deploy_path
        self.state_name = state_name
        self.start_timestamp = start_timestamp
        self.start_ts = start_ts
        self.capacity = capacity
        self.available = available
        self.used_size = used_size
        self.leader_count = leader_count
        self.leader_size = leader_size
        self.leader_score = leader_score
        self.leader_weight = leader_weight
        self.region_count = region_count
        self.region_size = region_size
        self.region_score = region_score
        self.region_weight = region_weight
        self.uptime = uptime

    @classmethod
    def from_api_all(cls, pd_addr):
        all_stores = list()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/stores" % pd_url, headers={"content-type": "application/json"})
        # pprint(resp.json())
        for store in resp.json()["stores"]:
            cls_kwargs = {}
            for k, v in store["status"].items():
                if k in ("sending_snap_count", "receiving_snap_count"):
                    continue
                else:
                    cls_kwargs[k] = v
            for k, v in store["store"].items():
                if k == "id":
                    cls_kwargs["store_id"] = v
                elif k == "state":
                    continue
                else:
                    cls_kwargs[k] = v
            all_stores.append(cls(**cls_kwargs))
        return all_stores

    @classmethod
    def from_api_ip(cls, pd_addr, ip):
        all_stores = list()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/stores" % pd_url, headers={"content-type": "application/json"})
        # pprint(resp.json())
        for store in resp.json()["stores"]:
            if store["store"]["address"].split(":")[0] == ip:
                cls_kwargs = {}
                for k, v in store["status"].items():
                    if k in ("sending_snap_count", "receiving_snap_count"):
                        continue
                    else:
                        cls_kwargs[k] = v
                for k, v in store["store"].items():
                    if k == "id":
                        cls_kwargs["store_id"] = v
                    elif k == "state":
                        continue
                    else:
                        cls_kwargs[k] = v
                all_stores.append(cls(**cls_kwargs))
        return all_stores

    @classmethod
    def from_api_storeid(cls, pd_addr, store_id):
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/store/%d" % (pd_url, store_id), headers={"content-type": "application/json"})
        # pprint(resp.json())
        store = resp.json()
        cls_kwargs = {}
        try:
            for k, v in store["status"].items():
                if k in ("sending_snap_count", "receiving_snap_count"):
                    continue
                else:
                    cls_kwargs[k] = v
            for k, v in store["store"].items():
                if k == "id":
                    cls_kwargs["store_id"] = v
                elif k == "state":
                    continue
                else:
                    cls_kwargs[k] = v
        except Exception:
            pprint(store)
            raise
        return cls(**cls_kwargs)


class Region(object):
    def __init__(self, region_id=None, start_key=None, end_key=None, epoch=None, peers=None, leader=None,
                 down_peers=None, pending_peers=None, written_bytes=None, read_bytes=None, written_keys=None,
                 read_keys=None, approximate_size=None, approximate_keys=None):
        self.region_id = region_id
        self.start_key = start_key
        self.end_key = end_key
        self.epoch = epoch
        self.peers = peers
        self.leader = leader
        self.down_peers = down_peers
        self.pending_peers = pending_peers
        self.written_bytes = written_bytes
        self.read_bytes = read_bytes
        self.written_keys = written_keys
        self.read_keys = read_keys
        self.approximate_size = approximate_size
        self.approximate_keys = approximate_keys

    @classmethod
    def from_api_all(cls, pd_addr):
        all_regions = list()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/regions" % pd_url, headers={"content-type": "application/json"})
        # pprint(resp.json())
        for region in resp.json()["regions"]:
            cls_kwargs = {}
            for k, v in region.items():
                if k == "id":
                    cls_kwargs["region_id"] = v
                else:
                    cls_kwargs[k] = v
            all_regions.append(cls(**cls_kwargs))
        return all_regions

    @classmethod
    def from_api_all_limit(cls, pd_addr, limit=5):
        all_regions = list()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/regions/readflow?limit=%d" % (pd_url, limit),
                            headers={"content-type": "application/json"})
        # pprint(resp.json())
        for region in resp.json()["regions"]:
            cls_kwargs = {}
            for k, v in region.items():
                if k == "id":
                    cls_kwargs["region_id"] = v
                else:
                    cls_kwargs[k] = v
            all_regions.append(cls(**cls_kwargs))
        return all_regions

    @classmethod
    def from_api_regionid(cls, pd_addr, region_id):
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/region/id/%d" % (pd_url, region_id), headers={"content-type": "application/json"})
        # pprint(resp.json())
        cls_kwargs = {}
        region = resp.json()
        if not region:
            print("Region %d not exist!" % region_id)
            exit(1)
        for k, v in region.items():
            if k == "id":
                cls_kwargs["region_id"] = v
            else:
                cls_kwargs[k] = v
        return cls(**cls_kwargs)

    @classmethod
    def from_api_storeid(cls, pd_addr, store_id):
        all_regions = list()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/regions/store/%d" % (pd_url, store_id), headers={"content-type": "application/json"})
        # pprint(resp.json())
        for region in resp.json()["regions"]:
            cls_kwargs = {}
            for k, v in region.items():
                if k == "id":
                    cls_kwargs["region_id"] = v
                else:
                    cls_kwargs[k] = v
            all_regions.append(cls(**cls_kwargs))
        return all_regions

    def remove_peer(self, pd_addr, store_id):
        # remove a region peer on specified store
        pd_url = "http://%s/pd/api/v1" % pd_addr
        for peer in self.peers:
            if peer["store_id"] == store_id:
                payload = {"name": "remove-peer", "region_id": self.region_id, "store_id": store_id}
                resp = requests.post("%s/operators" % pd_url,
                                     data=json.dumps(payload),
                                     headers={"content-type": "application/json"})
                if resp.status_code == 200:
                    return True, resp.text
                else:
                    return False, resp.text
            else:
                continue
        return False, "No peer on store %d for region %d!" % (store_id, self.region_id)


if __name__ == '__main__':
    args = argParse()
    optionHandler = OptionHandler(args.url, args.storeID, args.regionID, args.option, args.limit, args.interval)
    optionHandler.run()
