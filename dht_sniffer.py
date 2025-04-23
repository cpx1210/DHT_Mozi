# Extended from simDHT: https://github.com/old-woman/simDHT.git

import socket
from hashlib import sha1
from random import randint
from struct import unpack
from socket import inet_ntoa
from threading import Timer, Thread
from time import sleep
from collections import deque
from queue import Queue
import bencode_open
import traceback
# from bencode import bencode, bdecode
import bencodepy
from pymongo import MongoClient
import locale

locale.setlocale(locale.LC_ALL, '')
code = locale.getpreferredencoding()

BOOTSTRAP_NODES = (
    #("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    #("router.utorrent.com", 6881),
)

TID_LENGTH = 2
RE_JOIN_DHT_INTERVAL = 3
TOKEN_LENGTH = 2
Mozi_Prefix = b"888888" #Mozi节点ID硬编码以888888作为开头
Prefix = "888888"
Min_Prefix_length = 6


def entropy(length):
    return "".join(chr(randint(0, 255)) for _ in range(length))


#随机一个20的节点id
def random_id():
    h = sha1()
    h.update((Prefix + entropy(14)).encode())
    return h.digest()

#解析节点：0-20位是nid，20-24位是ipv4的ip，24-26位是端口号，
def decode_nodes(nodes):
    n = []
    length = len(nodes)
    if (length % 26) != 0:
        return n
    for i in range(0, length, 26):
        nid = nodes[i:i + 20]
        ip = inet_ntoa(nodes[i + 20:i + 24])
        port = unpack("!H", nodes[i + 24:i + 26])[0]
        n.append((nid, ip, port))
    return n


def timer(t, f):
    Timer(t, f).start()


def get_neighbor(target, nid, end=10):
    return target[:end] + nid[end:]


class KNode(object):
    def __init__(self, nid, ip, port):
        self.nid = nid
        self.ip = ip
        self.port = port


class DHTClient(Thread):
    def __init__(self, q, max_node_qsize):
        Thread.__init__(self)
        self.setDaemon(True)
        self.q = q
        self.max_node_qsize = max_node_qsize
        self.nid = random_id()
        self.nodes = deque(maxlen=max_node_qsize)


    def send_krpc(self, msg, address):
        try:
            # self.ufd.sendto(bencode(msg), address)
            self.ufd.sendto(bencodepy.encode(msg), address)
        except Exception as e:
            traceback.print_exc()

    def send_find_node(self, address, nid=None):
        nid = get_neighbor(nid, self.nid) if nid else self.nid
        tid = entropy(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {
                "id": nid,
                "target": random_id()
            }
        }
        self.send_krpc(msg, address)

    def join_DHT(self):
        for address in BOOTSTRAP_NODES:
            self.send_find_node(address)

    def re_join_DHT(self):
        if self.q.empty():
            if len(self.nodes) == 0:
                self.join_DHT()
            timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT)

    def auto_send_find_node(self):
        wait = 1.0 / self.max_node_qsize
        while self.q.empty():
            try:
                if len(self.nodes)==0:
                    continue
                node = self.nodes.popleft()
                self.send_find_node((node.ip, node.port), node.nid)
            except IndexError:
                traceback.print_exc()
            sleep(wait)
        self.q.put('dht client shutdown')
        return 0

#重要节点发现逻辑
    def process_find_node_response(self, msg, address):
        nodes = decode_nodes(msg[b"r"][b"nodes"])
        seen = set()   #创建一个集合储存已发现节点
        mozi_count = 0
        new_nodes = len(nodes)
        seen = set() #创建已发现节点的集合
        #print(f"[节点发现] 来自 {address} | 新增节点: {new_nodes}个")
        for node in nodes:
            (nid, ip, port) = node
            node_key = (nid, ip, port)
            if node_key in seen:       #去重机制
                continue
            if len(nid) != 20:
                continue
            if ip == self.bind_ip:
                continue
            if port < 1 or port > 65535:
                continue
            if nid.startswith(Mozi_Prefix):
                print(f"疑似节点 {ip}:{port},ID:{nid.hex()}")
                mozi_count += 1
            n = KNode(nid, ip, port)
            self.nodes.append(n)
            seen.add(node_key)


class DHTServer(DHTClient):
    def __init__(self, q, master, bind_ip, bind_port, max_node_qsize):
        DHTClient.__init__(self, q, max_node_qsize)
        self.q = q
        self.master = master
        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.cnt = 0
        self.process_request_actions = {
            "get_peers": self.on_get_peers_request,
            "announce_peer": self.on_announce_peer_request,
        }
        self.ufd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.bind_ip, self.bind_port))
        timer(RE_JOIN_DHT_INTERVAL, self.re_join_DHT)
        print(f"[DHT Server] 启动成功 | 监听地址: {bind_ip}:{bind_port}")
        print(f"[节点管理] 最大节点容量: {max_node_qsize}")

    def run(self):
        self.re_join_DHT()
        while self.q.empty():
            try:
                (data, address) = self.ufd.recvfrom(65536)
                # msg = bdecode(data)
                msg = bencodepy.decode(data)
                self.on_message(msg, address)
            except bencodepy.BencodeDecodeError:
                print(f'bdecode error: {data}')
            except Exception:
                traceback.print_exc()
        del self.ufd
        self.q.put('dht server shutdown')
        return 0

    def on_message(self, msg, address):
        try:
            if msg[b"y"] == b"r":
                # previous_nodes = len(self.nodes)
                if b"nodes" in msg[b"r"]:
                    self.process_find_node_response(msg, address)
                # print(f'krpc response received: found {len(self.nodes) - previous_nodes} nodes')
            elif msg[b"y"] == b"q":
                req_type = msg[b"q"].decode()
                try:
                    self.process_request_actions[msg[b"q"]](msg, address)
                    print(f'krpc request received: {msg[b"q"]}')
                except KeyError:
                    self.play_dead(msg, address)
        except Exception:
            traceback.print_exc()
#种子捕获
    def on_get_peers_request(self, msg, address):
        try:
            infohash = msg[b"a"][b"info_hash"]
            tid = msg[b"t"]
            nid = msg[b"a"][b"id"]
            token = infohash[:TOKEN_LENGTH]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(infohash, self.nid),
                    "nodes": "",
                    "token": token
                }
            }
            self.send_krpc(msg, address)
            print(f"[种子捕获] 来自 {address[0]} | InfoHash: {infohash.hex()}")
        except KeyError:
            traceback.print_exc()

    def on_announce_peer_request(self, msg, address):
        try:
            infohash = msg[b"a"][b"info_hash"]
            token = msg["a"]["token"]
            nid = msg["a"]["id"]
            tid = msg["t"]
            if nid.startswith(Mozi_Prefix):
                if b"implied_port" in msg[b"a"] and msg[b"a"][b"implied_port"] != 0:
                    port = address[1]
                else:
                    port = msg[b"a"][b"port"]
                    if port < 1 or port > 65535:
                        return
                self.cnt += 1
                # self.master.log(infohash, (address[0], port))
        except Exception:
            traceback.print_exc()
        finally:
            self.ok(msg, address)

    def play_dead(self, msg, address):
        try:
            tid = msg[b"t"]
            msg = {
                "t": tid,
                "y": "e",
                "e": [202, "Server Error"]
            }
            self.send_krpc(msg, address)
        except KeyError:
            traceback.print_exc()

    def ok(self, msg, address):
        try:
            tid = msg[b"t"]
            nid = msg[b"a"][b"id"]
            msg = {
                "t": tid,
                "y": "r",
                "r": {
                    "id": get_neighbor(nid, self.nid)
                }
            }
            self.send_krpc(msg, address)
        except KeyError:
            traceback.print_exc()


# class Master(object):
#     def __init__(self):
#         conn = MongoClient()
#         mongodht = conn['dht']
#         self.posts = mongodht.posts

#     def log(self, infohash, address=None):
#         q = self.posts.find_one({'_id': infohash.encode("hex")})
#         if q:
#             if address not in q['address']:
#                 tmpadr = q['address']
#                 tmpadr.append(address)
#                 self.posts.update({'_id': infohash.encode("hex")}, {'address': tmpadr})
#         else:
#             self.posts.insert({'_id': infohash.encode("hex"), 'address': [address]})


class Crawler(Thread):
    def __init__(self, q, port):
        Thread.__init__(self)
        self.setDaemon(True)
        self.q = q
        self.dht = DHTServer(self.q, None, "0.0.0.0", port, max_node_qsize=200)

    def run(self):
        self.dht.start()
        self.dht.auto_send_find_node()
        while self.q.empty():
            pass
        self.dht.join(0)

q = Queue()
dht = Crawler(q, 6000)
dht.start()