# coding=utf-8
# @Time: 2022/1/7 14:56
# @Author: forevermessi@foxmail.com
"""
get store/region info from pd http api and pretty print result
"""
import argparse
import json
import requests
from utils.logger import StreamLogger
from utils.formatter import Formatter
from utils.color import Color
from time import sleep
from pprint import pprint
from typing import List

# Const
ALL_SUPPORT_ACTIONS = ["showStore", "showStores", "showRegion", "showRegions", "showStoreRegions",
                       "showRegions1Peer", "showRegions2Peer", "showRegions3Peer", "showRegions4Peer",
                       "showRegionsNoLeader", "removeRegionPeer", "removeStorePeers"]

# TODO: 2 new actions -> safeRemoveRegionPeer, safeRemoveStorePeers: remove peer/peers when len(Region peers on Up
#  stores after removed)>=2

# Logger and Color
logger = StreamLogger()
color = Color()


# arg parse
def argParse():
    parser = argparse.ArgumentParser(description="pd http api store/region info formatter.")
    parser.add_argument("-u", metavar="<pd-addr>", dest="url", required=True, type=str,
                        help="pd addr, format ip:port, for example 127.0.0.1:2379")
    parser.add_argument("-o", metavar="<option>", dest="option", choices=ALL_SUPPORT_ACTIONS, default="showStores",
                        help="store/region options, default `showStores`, Options: %s" % ALL_SUPPORT_ACTIONS)
    parser.add_argument("-s", metavar="<store-id>", dest="storeID", type=int,
                        help="store id")
    parser.add_argument("-r", metavar="<region-id>", dest="regionID", type=int,
                        help="region id")
    parser.add_argument("-l", metavar="<limit>", dest="limit", type=int, default=5,
                        help="region show limit, default 5")
    parser.add_argument("-t", metavar="<interval>", dest="interval", type=int,
                        default=3, help="operator create interval(seconds), default 3")
    return parser.parse_args()


class OptionHandler(object):
    def __init__(self, url: str, storeID: int, regionID: int, option: str, limit: int, interval: int):
        self.__url = url
        self.__storeID = storeID
        self.__regionID = regionID
        self.__option = option
        self.__limit = limit
        self.__interval = interval
        self.__store_formatter = Formatter(
            column_definition={
                "StoreAddr": 25, "StoreID": 15, "State": 15, "LCt/RCt": 15, "LWt/RWt": 10, "StartTime": 30,
                "Labels": 64
            })
        self.__region_formatter = Formatter(
            column_definition={
                "RegionID": 15, "StoreList": 40, "Leader": 15, "LeaderAddr": 30, "DownPeersStoreID": 25,
                "PendingPeersStoreID": 25, "Size(MB)": 10, "Keys": 10
            })

    # for single store
    def showStore(self):
        if not self.__storeID:
            print("Error: Store ID should be specified!")
            exit(1)
        replication_config = requests.get("http://%s/pd/api/v1/config" % self.__url).json().get('replication')
        label_rules = replication_config.get('location-labels')
        label_force_match = replication_config.get('strictly-match-label')
        color.print_red("# Location-Label Rules: {0} (force: {1})".format(label_rules, label_force_match))
        stores = Store.from_api_all(pd_addr=self.__url, all_state=False)
        self.__store_formatter.print_header()
        for store in stores:
            if store.store_id == self.__storeID:
                self.__store_formatter.print_line((store.address, store.store_id, store.state_name,
                                                   "%s/%s" % (store.leader_count, store.region_count),
                                                   "%s/%s" % (store.leader_weight, store.region_weight),
                                                   store.start_ts,
                                                   [{l.get('key'): l.get('value')} for l in store.labels])
                                                  )
                return
        print("Error: Store ID %d not exist!" % self.__storeID)
        exit(1)

    # for all stores
    def showStores(self):
        replication_config = requests.get("http://%s/pd/api/v1/config" % self.__url).json().get('replication')
        label_rules = replication_config.get('location-labels')
        label_force_match = replication_config.get('strictly-match-label')
        color.print_red("# Location-Label Rules: {0} (force: {1})".format(label_rules, label_force_match))
        stores = Store.from_api_all(pd_addr=self.__url, all_state=False)
        self.__store_formatter.print_header()
        for store in sorted(stores, key=lambda s: s.address):
            self.__store_formatter.print_line((store.address, store.store_id, store.state_name,
                                               "%s/%s" % (store.leader_count, store.region_count),
                                               "%s/%s" % (store.leader_weight, store.region_weight),
                                               store.start_ts,  [{l.get('key'): l.get('value')} for l in store.labels]))

    # for single region
    def showRegion(self):
        if not self.__regionID:
            print("Error: Region ID should be specified!")
            exit(1)
        region: Region = Region.from_api_regionid(pd_addr=self.__url, region_id=self.__regionID)
        self.__region_formatter.print_header()
        storeList: List[int] = [p["store_id"] for p in region.peers]
        leader: int = region.leader.get("store_id") if region.leader else None
        leaderAddr: str = Store.from_api_storeid(pd_addr=self.__url, store_id=leader).address if leader else None
        if region.down_peers:
            downPeersStoreID = [p["peer"]["store_id"] for p in region.down_peers]
        else:
            downPeersStoreID = []
        if region.pending_peers:
            PendingPeersStoreID = [p["store_id"] for p in region.pending_peers]
        else:
            PendingPeersStoreID = []
        self.__region_formatter.print_line(((region.region_id, storeList, leader,
                                             leaderAddr, downPeersStoreID,
                                             PendingPeersStoreID, region.approximate_size,
                                             region.approximate_keys)))

    # for all regions(default show limit 5)
    def showRegions(self):
        regions = Region.from_api_all_limit(pd_addr=self.__url, limit=self.__limit)
        self.__region_formatter.print_header()
        for region in regions:
            storeList = [p["store_id"] for p in region.peers]
            leader = region.leader.get("store_id") if region.leader else None
            leaderAddr: str = Store.from_api_storeid(pd_addr=self.__url, store_id=leader).address if leader else None
            if region.down_peers:
                downPeersStoreID = [p["peer"]["store_id"] for p in region.down_peers]
            else:
                downPeersStoreID = []
            if region.pending_peers:
                PendingPeersStoreID = [p["store_id"] for p in region.pending_peers]
            else:
                PendingPeersStoreID = []
            self.__region_formatter.print_line((region.region_id, storeList, leader,
                                                leaderAddr, downPeersStoreID,
                                                PendingPeersStoreID,
                                                region.approximate_size,
                                                region.approximate_keys))

    def showRegionsNPeer(self, n):
        regions = Region.from_api_all(pd_addr=self.__url)
        color.print_red("# {0}PeerRegions(limit {1}):".format(n, self.__limit))
        self.__region_formatter.print_header()
        i = j = 0
        while i < len(regions) and j < self.__limit:
            region = regions[i]
            if len(region.peers) == n:
                storeList = [p["store_id"] for p in region.peers]
                leader = region.leader.get("store_id") if region.leader else None
                leaderAddr: str = Store.from_api_storeid(pd_addr=self.__url,
                                                         store_id=leader).address if leader else None
                if region.down_peers:
                    downPeersStoreID = [p["peer"]["store_id"] for p in region.down_peers]
                else:
                    downPeersStoreID = []
                if region.pending_peers:
                    PendingPeersStoreID = [p["store_id"] for p in region.pending_peers]
                else:
                    PendingPeersStoreID = []
                self.__region_formatter.print_line((region.region_id, storeList, leader,
                                                    leaderAddr, downPeersStoreID,
                                                    PendingPeersStoreID,
                                                    region.approximate_size,
                                                    region.approximate_keys))
                j += 1
            i += 1

    def showRegionsNoLeader(self):
        regions = Region.from_api_all(pd_addr=self.__url)
        color.print_red("# RegionsNoLeader(limit {0}):".format(self.__limit))
        self.__region_formatter.print_header()
        i = j = 0
        while i < len(regions) and j < self.__limit:
            region = regions[i]
            if not region.leader:
                storeList = [p["store_id"] for p in region.peers]
                leader = region.leader.get("store_id") if region.leader else None
                leaderAddr: str = Store.from_api_storeid(pd_addr=self.__url,
                                                         store_id=leader).address if leader else None
                if region.down_peers:
                    downPeersStoreID = [p["peer"]["store_id"] for p in region.down_peers]
                else:
                    downPeersStoreID = []
                if region.pending_peers:
                    PendingPeersStoreID = [p["store_id"] for p in region.pending_peers]
                else:
                    PendingPeersStoreID = []
                self.__region_formatter.print_line((region.region_id, storeList, leader,
                                                    leaderAddr, downPeersStoreID,
                                                    PendingPeersStoreID,
                                                    region.approximate_size,
                                                    region.approximate_keys))
                j += 1
            i += 1

    # for all regions in a given store
    def showStoreRegions(self):
        if not self.__storeID:
            print("Error: Store ID should be specified!")
            exit(1)
        regions = Region.from_api_storeid(pd_addr=self.__url, store_id=self.__storeID)
        color.print_red("# Top {0} Regions(limit {0}):".format(self.__limit))
        self.__region_formatter.print_header()
        for region in regions[:self.__limit]:
            storeList = [p["store_id"] for p in region.peers]
            leader = region.leader.get("store_id") if region.leader else None
            leaderAddr: str = Store.from_api_storeid(pd_addr=self.__url, store_id=leader).address if leader else None
            if region.down_peers:
                downPeersStoreID = [p["peer"]["store_id"] for p in region.down_peers]
            else:
                downPeersStoreID = []
            if region.pending_peers:
                PendingPeersStoreID = [p["store_id"] for p in region.pending_peers]
            else:
                PendingPeersStoreID = []
            self.__region_formatter.print_line((region.region_id, storeList, leader,
                                                leaderAddr, downPeersStoreID,
                                                PendingPeersStoreID,
                                                region.approximate_size,
                                                region.approximate_keys))

    def removeRegionPeer(self):
        if not self.__storeID or not self.__regionID:
            print("Error: Both Store ID and Region ID should be specified!")
            exit(1)
        region = Region.from_api_regionid(pd_addr=self.__url, region_id=self.__regionID)
        isSuccess, respText = region.remove_peer(pd_addr=self.__url, store_id=self.__storeID)
        if isSuccess:
            logger.info("Operator remove-peer created for region %d's peer on store %d[1/1]." % (self.__regionID,
                                                                                                 self.__storeID))
        else:
            logger.error(respText)

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
        elif self.__option == "showRegions1Peer":
            self.showRegionsNPeer(n=1)
        elif self.__option == "showRegions2Peer":
            self.showRegionsNPeer(n=2)
        elif self.__option == "showRegions3Peer":
            self.showRegionsNPeer(n=3)
        elif self.__option == "showRegions4Peer":
            self.showRegionsNPeer(n=4)
        elif self.__option == "showRegionsNoLeader":
            self.showRegionsNoLeader()
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
                 region_score=None, region_weight=None, uptime=None, slow_score=None):
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
        self.slow_score = slow_score

    @classmethod
    def from_api_all(cls, pd_addr, all_state=True):
        store_proto = Store()
        all_stores = list()
        if all_state:
            pd_url = "http://%s/pd/api/v1/stores?state=0&state=1&state=2" % pd_addr
        else:
            pd_url = "http://%s/pd/api/v1/stores" % pd_addr
        resp = requests.get(pd_url, headers={"content-type": "application/json"})
        # pprint(resp.json())
        if resp.status_code != 200:
            raise Exception(resp.text)
        for store in resp.json()["stores"]:
            cls_kwargs = {}
            for k, v in store["status"].items():
                if k not in store_proto.__dir__():
                    continue
                else:
                    cls_kwargs[k] = v
            for k, v in store["store"].items():
                if k == "id":
                    cls_kwargs["store_id"] = v
                elif k not in store_proto.__dir__():
                    continue
                else:
                    cls_kwargs[k] = v
            all_stores.append(cls(**cls_kwargs))
        return all_stores

    @classmethod
    def from_api_ip(cls, pd_addr, ip, all_state=True):
        store_proto = Store()
        all_stores = list()
        if all_state:
            pd_url = "http://%s/pd/api/v1/stores?state=0&state=1&state=2" % pd_addr
        else:
            pd_url = "http://%s/pd/api/v1/stores" % pd_addr
        resp = requests.get(pd_url, headers={"content-type": "application/json"})
        if resp.status_code != 200:
            raise Exception(resp.text)
        # pprint(resp.json())
        for store in resp.json()["stores"]:
            if store["store"]["address"].split(":")[0] == ip:
                cls_kwargs = {}
                for k, v in store["status"].items():
                    if k not in store_proto.__dir__():
                        continue
                    else:
                        cls_kwargs[k] = v
                for k, v in store["store"].items():
                    if k == "id":
                        cls_kwargs["store_id"] = v
                    elif k not in store_proto.__dir__():
                        continue
                    else:
                        cls_kwargs[k] = v
                all_stores.append(cls(**cls_kwargs))
        return all_stores

    @classmethod
    def from_api_storeid(cls, pd_addr, store_id):
        store_proto = Store()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/store/%d" % (pd_url, store_id), headers={"content-type": "application/json"})
        # pprint(resp.json())
        if resp.status_code != 200:
            raise Exception(resp.text)
        store = resp.json()
        cls_kwargs = {}
        try:
            for k, v in store["status"].items():
                if k not in store_proto.__dir__():
                    continue
                else:
                    cls_kwargs[k] = v
            for k, v in store["store"].items():
                if k == "id":
                    cls_kwargs["store_id"] = v
                elif k not in store_proto.__dir__():
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
        region_proto = Region()
        all_regions = list()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/regions" % pd_url, headers={"content-type": "application/json"})
        if resp.status_code != 200:
            raise Exception(resp.text)
        # pprint(resp.json())
        for region in resp.json()["regions"]:
            cls_kwargs = {}
            for k, v in region.items():
                if k == "id":
                    cls_kwargs["region_id"] = v
                elif k not in region_proto.__dir__():
                    continue
                else:
                    cls_kwargs[k] = v
            all_regions.append(cls(**cls_kwargs))
        return all_regions

    @classmethod
    def from_api_all_limit(cls, pd_addr, limit=5):
        region_proto = Region()
        all_regions = list()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/regions/readflow?limit=%d" % (pd_url, limit),
                            headers={"content-type": "application/json"})
        if resp.status_code != 200:
            raise Exception(resp.text)
        # pprint(resp.json())
        for region in resp.json()["regions"]:
            cls_kwargs = {}
            for k, v in region.items():
                if k == "id":
                    cls_kwargs["region_id"] = v
                elif k not in region_proto.__dir__():
                    continue
                else:
                    cls_kwargs[k] = v
            all_regions.append(cls(**cls_kwargs))
        return all_regions

    @classmethod
    def from_api_regionid(cls, pd_addr, region_id):
        region_proto = Region()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/region/id/%d" % (pd_url, region_id), headers={"content-type": "application/json"})
        # pprint(resp.json())
        if resp.status_code != 200:
            raise Exception(resp.text)
        cls_kwargs = {}
        region = resp.json()
        if not region:
            print("Region %d not exist!" % region_id)
            exit(1)
        for k, v in region.items():
            if k == "id":
                cls_kwargs["region_id"] = v
            elif k not in region_proto.__dir__():
                continue
            else:
                cls_kwargs[k] = v
        return cls(**cls_kwargs)

    @classmethod
    def from_api_storeid(cls, pd_addr, store_id):
        region_proto = Region()
        all_regions = list()
        pd_url = "http://%s/pd/api/v1" % pd_addr
        resp = requests.get("%s/regions/store/%d" % (pd_url, store_id), headers={"content-type": "application/json"})
        if resp.status_code != 200:
            raise Exception(resp.text)
        # pprint(resp.json())
        for region in resp.json()["regions"]:
            cls_kwargs = {}
            for k, v in region.items():
                if k == "id":
                    cls_kwargs["region_id"] = v
                elif k not in region_proto.__dir__():
                    continue
                else:
                    cls_kwargs[k] = v
            all_regions.append(cls(**cls_kwargs))
        return all_regions

    def remove_peer(self, pd_addr, store_id) -> (bool, str):
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

