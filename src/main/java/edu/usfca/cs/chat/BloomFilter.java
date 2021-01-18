package edu.usfca.cs.chat;

import com.sangupta.murmur.Murmur3;

import java.util.BitSet;

public class BloomFilter {
    private BitSet bitSet;

    BloomFilter(int size){
        bitSet = new BitSet(size);
    }

    public void put(String key){
        int[] ind = hashKey(key);
        bitSet.set(ind[0]);
        bitSet.set(ind[1]);
        bitSet.set(ind[2]);
    }

    public boolean get(String key){
        int[] ind = hashKey(key);
        if(!bitSet.get(ind[0]) || !bitSet.get(ind[1]) || !bitSet.get(ind[2]))
                return false;
        return true;
    }

    public int[] hashKey(String key){
        int[] ar = new int[3];
        byte[] bytes = key.getBytes();
        long seed = key.length();
        long hash1 = Murmur3.hash_x86_32(bytes, bytes.length, seed);
        long hash2 = Murmur3.hash_x86_32(bytes, bytes.length, hash1);
        for(int x = 0;x < 3;x++){
            ar[x] = (int)((hash1 + (x * hash2)) % this.bitSet.size());
        }

        return ar;
    }

    @Override
    public String toString() {
        return "BloomFilter{" +
                "bitSet=" + bitSet +
                '}';
    }
}
