#include "blimit.hpp"

#include <iostream>
#include <string>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <queue>
#include <deque>
#include <ctime>
#include <thread>
#include <mutex>
#include <algorithm>
#include <map>

using namespace std;

vector<int> indexToReal;

class comparator {
public:
    int operator() (const pair<int, int>& p1, const pair<int, int>& p2)
    {
        if (p1.first == p2.first) {
            return indexToReal[p1.second] > indexToReal[p2.second];
        } else {
            return p1.first > p2.first;
        }
    }
};

vector<vector<pair<int, int>>> neighbours;

unordered_map<int, int> realToIndex;

vector<unique_ptr<priority_queue<pair<int, int>, vector<pair<int, int>>, comparator>>> s;
vector<unique_ptr<set<pair<int, int>, comparator>>> nt;

vector<unique_ptr<mutex> > mutexesS, mutexesT, mutexesQ;

mutex barier;
int onBarier;
mutex wait;

pair<int, int> inline last(int method, int v) {
    if (s[v]->empty()) {
        return make_pair(-1, -1);
    } else {
        if (s[v]->size() < bvalue(method, indexToReal[v])) {
            return make_pair(-1, -1);
        } else {
            return s[v]->top();
        }
    }
}

// (u, v) :<: (v, y)
bool inline lessThan(int a, int b, int u, int y) {

    if (a == b) {
        if (u != -1 && y != -1) {
            return indexToReal[u] < indexToReal[y];
        } else if (u == -1) {
            return true;
        } else {
            return false;
        }
    } else {
        return a < b;
    }
}

// return arg max(v) {W(u,v) : (v in N[u] - T[u]) && (W(u,v) :>: W(v, S[v].last))};
pair<int, int> inline maximum(int method, int u) {
    if (nt[u]->empty()) {
        return make_pair(-1, -1);
    }

    auto end = nt[u]->end();
    for (auto it = nt[u]->begin(); it != end; it++) {
        if (bvalue(method, indexToReal[it->second]) != 0) {
            pair<int, int> vLast = last(method, it->second);

            if (lessThan(vLast.first, it->first, vLast.second, u)) {
                return *it;
            }
        }
    }

    return make_pair(-1, -1);
}

mutex mutexQ;
int remains = 0;
int from = 0;
int threadsCount = 0;
int access = 0;

pair<int, int> getAvailableIndexes() {
    pair<int, int> result = make_pair(-1, -1);
    mutexQ.lock();
    access++;
    if (remains > 0) {
        int count = max(remains / threadsCount, 1);
        result = make_pair(from, (from + count));
        remains -= count;
        from += count;
    }
    mutexQ.unlock();
    return result;
}

void run(int from, int to, int b_method, int *sum, bool initialize) {
    deque<int> q;

    pair<int, int> range = getAvailableIndexes();

    for (int i = range.first; i < range.second; i++) {
        q.push_back(i);
    }

    if (initialize) {
        for (int i = from; i < to; i++) {
            nt[i]->insert(neighbours[i].begin(), neighbours[i].end());
        }
    }

    barier.lock();
    onBarier++;
    if (onBarier < threadsCount) {
        barier.unlock();
        wait.lock();
    }
    onBarier--;
    if (onBarier > 0) {
        wait.unlock();
    } else {
        barier.unlock();
    }

    while (!q.empty()) {

        int u = q.front();
        q.pop_front();

        mutexesQ[u]->lock();

        while ((neighbours[u].size() - nt[u]->size()) < bvalue(b_method, indexToReal[u])) {
            pair<int, int> valueVertexX = maximum(b_method, u);
            int uxWeight = valueVertexX.first;
            int x = valueVertexX.second;

            if (x == -1) {
                break;
            } else { //u will adorate x
                mutexesS[x]->lock();
                int still = maximum(b_method, u).second;

                if (still == x) {
                    pair<int, int> y = last(b_method, x);

                    s[x]->emplace(uxWeight, u);

                    mutexesT[u]->lock();
                    nt[u]->erase(make_pair(uxWeight, x));
                    mutexesT[u]->unlock();

                    if (y.second != -1) {
                        mutexesT[y.second]->lock();
                        nt[y.second]->insert(make_pair(y.first, x));
                        mutexesT[y.second]->unlock();

                        s[x]->pop();
                        q.push_back(y.second);
                    }
                }

                mutexesS[x]->unlock();
            }
        }

        mutexesQ[u]->unlock();

        if (q.empty()) {
            range = getAvailableIndexes();

            if (range.first != -1) {
                for (int i = range.first; i < range.second; i++) {
                    q.push_back(i);
                }
            }
        }

    }

    barier.lock();
    onBarier++;
    if (onBarier < threadsCount) {
        barier.unlock();
        wait.lock();
    }
    onBarier--;
    if (onBarier > 0) {
        wait.unlock();
    } else {
        barier.unlock();
    }

    int local = 0;
    for (int i = from; i < to; i++) {
        while (!s[i]->empty()) {
            pair<int, int> top = s[i]->top();

            local += top.first;
            nt[i]->insert(top);

            s[i]->pop();
        }
    }

    *sum = local;

}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        cerr << "usage: "<<argv[0]<<" thread-count inputfile b-limit"<< std::endl;
        return 1;
    }

    threadsCount = std::stoi(argv[1]);
    int bLimit = std::stoi(argv[3]);
    string inputFilename{argv[2]};

    ifstream inputStream;
    inputStream.open(inputFilename);

    clock_t beforeFileRead = clock();

    int currentIndex = 0;
    int totalSize = 0;
    for (string line; getline(inputStream, line); ) {

        if (line[0] != '#') {

            int x = 0, y = 0, weight = 0;
            int spaces = 0;

            for (int i = 0; i < line.size(); i++) {
                if (line[i] == ' ') {
                    spaces ++;
                } else if (spaces == 0) {
                    x *= 10;
                    x += line[i] - '0';
                } else if (spaces == 1) {
                    y *= 10;
                    y += line[i] - '0';
                } else if (spaces == 2) {
                    weight *= 10;
                    weight += line[i] - '0';
                }
            }

            auto xIndexIter = realToIndex.find(x);
            if (xIndexIter == realToIndex.end()) {
                xIndexIter = realToIndex.emplace(x, currentIndex).first;

                vector<pair<int, int>> empty;
                neighbours.push_back(empty);

                indexToReal.push_back(x);

                currentIndex++;
            }
            int xIndex = xIndexIter->second;

            auto yIndexIter = realToIndex.find(y);
            if (yIndexIter == realToIndex.end()) {
                yIndexIter = realToIndex.emplace(y, currentIndex).first;

                vector<pair<int, int>> empty;
                neighbours.push_back(empty);

                indexToReal.push_back(y);

                currentIndex++;
            }
            int yIndex = yIndexIter->second;

            neighbours[xIndex].emplace_back(weight, yIndex);
            neighbours[yIndex].emplace_back(weight, xIndex);
        }
    }

    totalSize = currentIndex + 1;

    threadsCount = min(threadsCount, totalSize);

    thread threads[threadsCount];
    int* sums[threadsCount];

    long long size = totalSize / threadsCount;
    for (int i = 0; i < threadsCount; i++) {
        sums[i] = new int;
        *(sums[i]) = 0;
    }

    mutexesS.reserve(totalSize);
    mutexesT.reserve(totalSize);
    mutexesQ.reserve(totalSize);
    s.reserve(totalSize);
    nt.reserve(totalSize);

    for (int i = 0; i < totalSize; i++) {
        mutexesS.push_back(make_unique<mutex>());
        mutexesT.push_back(make_unique<mutex>());
        mutexesQ.push_back(make_unique<mutex>());

        s.push_back(make_unique<priority_queue<pair<int, int>, vector<pair<int, int>>, comparator>>());
        nt.push_back(make_unique<set<pair<int, int>, comparator>>());
    }

    clock_t afterFileRead = clock();
    double elapsed_secs = double(afterFileRead - beforeFileRead) / CLOCKS_PER_SEC;
    cout << "file read took: " << elapsed_secs << endl;
    wait.lock();

    bool initizlize = true;
    for (int b_method = 0; b_method < bLimit + 1; b_method++) {
        clock_t begin = clock();

        from = 0;
        remains = totalSize;
        access = 0;

        for (int i = 1; i < threadsCount; i++) {
            int from = size * i;
            int to;
            if (i == threadsCount - 1) {
                to = totalSize;
            } else {
                to = size * (i + 1);
            }

            threads[i] = thread {run, from, to, b_method, sums[i], initizlize};
        }

        run(0, size, b_method, sums[0], initizlize);

        for (int i = 1; i < threadsCount; i++) {
            threads[i].join();
        }

        int sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            sum += *(sums[i]);
        }

        sum /= 2;

        clock_t end = clock();
        elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
        cout <<  sum << " found in: " << elapsed_secs << " accessed: " << access << endl;

        initizlize = false;
    }

    clock_t wholeEnd = clock();
    elapsed_secs = double(wholeEnd - beforeFileRead) / CLOCKS_PER_SEC;
    cout << "everyting took: " << elapsed_secs << endl;

    elapsed_secs = double(wholeEnd - afterFileRead) / CLOCKS_PER_SEC;
    cout << "alghorithym took: " << elapsed_secs << endl;
}
