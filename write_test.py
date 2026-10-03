#!/usr/bin/env python3
import subprocess, sys, os, time, json, urllib.request, threading
from concurrent import futures

os.chdir('/tmp/write_test')

subprocess.run([sys.executable, '-m', 'grpc_tools.protoc',
    '--python_out=.', '--grpc_python_out=.', '-I.', 'raftkv.proto'],
    check=True)

import raftkv_pb2 as pb
import raftkv_pb2_grpc as pb_grpc
import grpc

HTTP_PORTS = ['9001', '9002', '9003', '9104', '9105']
HTTP_TO_GRPC = {'9001':'9500', '9002':'9501', '9003':'9502', '9104':'9604', '9105':'9605'}

def find_leader():
    for p in HTTP_PORTS:
        try:
            resp = urllib.request.urlopen(f'http://127.0.0.1:{p}/raft/status', timeout=2)
            status = json.loads(resp.read())
            if status.get('state') == 'Leader':
                return p, int(status.get('term', 0)), status.get('leader_id', '')
        except:
            continue
    return None, 0, ''

leader_port, term, leader_id = find_leader()
if not leader_port:
    print('ERROR: No leader found')
    sys.exit(1)

grpc_port = HTTP_TO_GRPC[leader_port]
print(f'Leader={leader_id}, term={term}, HTTP port={leader_port}, gRPC port={grpc_port}')

total = 100000
concurrency = 50
success = 0
fail = 0
errors = {}
lock = threading.Lock()
seq = [0]

def worker(worker_id):
    global success, fail
    local_seq = 0
    channel = grpc.insecure_channel(f'127.0.0.1:{grpc_port}')
    stub = pb_grpc.RaftServiceStub(channel)
    while True:
        with lock:
            seq[0] += 1
            n = seq[0]
        if n > total:
            break
        local_seq += 1
        entry = pb.LogEntry(
            term=term,
            index=n,
            command=json.dumps({'src': f'write-tester-{worker_id}', 'idx': local_seq, 'n': n}).encode()
        )
        req = pb.AppendEntriesRequest(
            term=term,
            leader_id=leader_id,
            prev_log_index=0,
            prev_log_term=0,
            entries=[entry],
            leader_commit=n
        )
        try:
            resp = stub.AppendEntries(req, timeout=3.0)
            if resp.success:
                with lock:
                    success += 1
            else:
                with lock:
                    fail += 1
                    if fail <= 3:
                        print(f'[FAIL] n={n} resp.term={resp.term} resp.success={resp.success}')
        except Exception as e:
            with lock:
                fail += 1
                if fail <= 3:
                    print(f'[ERROR] n={n} err={e}')
        channel.close()
        channel = grpc.insecure_channel(f'127.0.0.1:{grpc_port}')
        stub = pb_grpc.RaftServiceStub(channel)

print(f'Starting {total} AppendEntries writes with {concurrency} workers...')
t0 = time.time()

with futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
    list(pool.map(worker, range(concurrency)))

elapsed = time.time() - t0
print(f'\n{"="*60}')
print(f'  gRPC AppendEntries Write Test Results')
print(f'{"="*60}')
print(f'  Total sent    : {total}')
print(f'  Success       : {success} ({success/total*100:.2f}%)')
print(f'  Failed        : {fail} ({fail/total*100:.2f}%)')
print(f'  TPS           : {total/elapsed:.0f} req/s')
print(f'  Elapsed       : {elapsed:.2f}s')
print(f'  Leader        : {leader_id} (term={term})')
print(f'  gRPC target   : 127.0.0.1:{grpc_port}')
print(f'{"="*60}')