package node;

import java.security.KeyStore;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class LinkedHashMapDemo {
  /*  public static void main(String[] args) {
        LinkedHashTable linkedHashTable = new LinkedHashTable();
        linkedHashTable.put();
        HashMap<Object, Object> objectObjectHashMap = new HashMap<>();
        KeyStore.Entry entry = new KeyStore.Entry();
        objectObjectHashMap.
*/

}

/*class Hashnode{
    public HashMap ht;
    public Hashnode before;
    public Hashnode after;
    public Hashnode(HashMap kv){
        this.ht = kv;
    }

}

class LinkedHashTable{
    public Map.Entry kv = new Map.Entry() {
        @Override
        public Object getKey() {
            return null;
        }

        @Override
        public Object getValue() {
            return null;
        }

        @Override
        public Object setValue(Object value) {
            return null;
        }
    };
    private Hashnode header = new Hashnode(kv);
    public void LinkedHashTable(){
        header.before = header;
        header.after = header;
    }

    //添加的方法
    public void put(Object key, Object value){

        kv.put(key,value);
        Hashnode newHashnode = new Hashnode(kv);
        Hashnode cur = header;
        while(cur.after!=null){
            cur = cur.after;
        }
        cur.after = newHashnode;
        newHashnode.before = cur;
    }

    //get方法
    public Object getKey(Object key){
        Object value = kv.getOrDefault(key, -1);
        if (!value.equals(-1)){
            Hashnode cur = header.after;
            while(cur !=null){
                if (cur.entry.get())
            }
        }

        return value;
    }
}*/

class LRUCache {
    HashMap<Integer, Node> map = new HashMap();
    int capacity;
    Node header;

    class Node {
        Node before;
        Node after;
        Integer key;
        Integer value;

        public Node(Integer key, Integer value) {
            this.key = key;
            this.value = value;
            before = this;
            after = this;
        }
    }


    public LRUCache(int capacity) {
        header = new Node(-1, -1);
        this.capacity = capacity;
    }

    public int get(int key) {
        Node node = null;
        if (map.containsKey(key)) {
            node = map.get(key);
            refresh(node);
        }
        return node.value;
    }

    public void put(int key, int value) {
        if (map.containsKey(key)) {
            Node node = map.get(key);
            node.value = value;
            refresh(node);
        }
        Node newNode = new Node(key, value);
        map.put(key, newNode);
        refresh(newNode);
        if (map.size() > capacity) {
            del();
        }
    }

    public void refresh(Node node) {
        node.after.before = node.before;
        node.before.after = node.after;
        header.after.before = node;
        node.after = header.after;
        node.before = header;
        header.after = node;
    }

    public void del() {
        Node node = header.before;
        node.before.after = header;
        header.before = node.before;
        map.remove(node.key);
    }
}