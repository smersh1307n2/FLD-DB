using System.Buffers.Binary;
namespace Fld1.Storage.Engine;

internal readonly record struct StorageTreeKeyReference(ulong Offset,int Length);
internal readonly record struct StorageTreeValueReference(ulong Offset,int Length,bool IsTombstone);
internal readonly record struct StorageTreeEntry(StorageTreeKeyReference Key,StorageTreeValueReference Value);
internal readonly record struct StorageTreeBuildEntry(ReadOnlyMemory<byte> Key,ulong KeyOffset,ulong ValueOffset,int ValueLength,bool IsTombstone);
internal readonly record struct StorageTreeLeafEntry(StorageTreeKeyReference Key,StorageTreeValueReference Value);
internal readonly record struct StorageTreeChild(ulong PageId,StorageTreeKeyReference FirstKey);
internal sealed record StorageTreeLeafPage(ulong NextPageId,StorageTreeLeafEntry[] Entries);
internal readonly struct StorageBTreeLeafPageView
{
    private readonly ReadOnlyMemory<byte> _payload;
    public ulong NextPageId { get; }
    public int Count { get; }

    public StorageBTreeLeafPageView(ReadOnlyMemory<byte> payload, ulong nextPageId, int count)
    {
        _payload = payload;
        NextPageId = nextPageId;
        Count = count;
    }

    public StorageTreeLeafEntry GetEntry(int index)
    {
        if ((uint)index >= (uint)Count) throw new ArgumentOutOfRangeException(nameof(index));
        return StorageBTreePageCodec.ReadLeafEntry(_payload.Span, index);
    }
}
internal sealed record StorageTreeInternalPage(ulong FirstChild,StorageTreeChild[] Separators);

internal static class StorageBTreePageCodec
{
    private const byte Version=1;
    private const int Header=12,LeafEntry=25,InternalEntry=20;
    private const int Capacity=StoragePageCodec.PageSize-StoragePageCodec.HeaderSize;
    public static int MaxLeafEntries=>(Capacity-Header)/LeafEntry;
    public static int MaxInternalChildren=>(Capacity-Header)/InternalEntry+1;

    public static byte[] EncodeLeaf(ulong generation,ulong pageId,ulong next,IReadOnlyList<StorageTreeLeafEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if(entries.Count is <1 || entries.Count>MaxLeafEntries) throw new ArgumentOutOfRangeException(nameof(entries));
        byte[] p=new byte[Header+entries.Count*LeafEntry];p[0]=Version;
        BinaryPrimitives.WriteUInt16LittleEndian(p.AsSpan(2),checked((ushort)entries.Count));
        BinaryPrimitives.WriteUInt64LittleEndian(p.AsSpan(4),next);int o=Header;
        foreach(var e in entries)
        {
            WriteKey(p.AsSpan(o,12),e.Key);o+=12;
            if(e.Value.Length<0 || (e.Value.IsTombstone && e.Value.Length!=0))throw new InvalidDataException("Invalid value reference.");
            _=checked(e.Value.Offset+(ulong)e.Value.Length);
            BinaryPrimitives.WriteUInt64LittleEndian(p.AsSpan(o),e.Value.Offset);
            BinaryPrimitives.WriteInt32LittleEndian(p.AsSpan(o+8),e.Value.Length);
            p[o+12]=e.Value.IsTombstone?(byte)1:(byte)0;o+=13;
        }
        return StoragePageCodec.Encode(generation,pageId,StoragePageKind.Leaf,p);
    }
    public static byte[] EncodeInternal(ulong generation,ulong pageId,ulong first,IReadOnlyList<StorageTreeChild> separators)
    {
        ArgumentNullException.ThrowIfNull(separators);
        if(first==0 || separators.Count>MaxInternalChildren-1)throw new ArgumentOutOfRangeException(nameof(first));
        byte[] p=new byte[Header+separators.Count*InternalEntry];p[0]=Version;
        BinaryPrimitives.WriteUInt16LittleEndian(p.AsSpan(2),checked((ushort)separators.Count));
        BinaryPrimitives.WriteUInt64LittleEndian(p.AsSpan(4),first);int o=Header;
        foreach(var c in separators)
        {
            if(c.PageId==0)throw new InvalidDataException("Null B+ tree child.");
            WriteKey(p.AsSpan(o,12),c.FirstKey);BinaryPrimitives.WriteUInt64LittleEndian(p.AsSpan(o+12),c.PageId);o+=InternalEntry;
        }
        return StoragePageCodec.Encode(generation,pageId,StoragePageKind.Internal,p);
    }
    public static ulong SelectInternalChild(
        ReadOnlyMemory<byte> bytes, ulong generation, ulong pageId, ReadOnlySpan<byte> searchKey,
        Func<StorageTreeKeyReference, byte[]> readKey)
        => SelectInternalChild(StoragePageCodec.Decode(bytes, generation, pageId), searchKey, readKey);

    public static ulong SelectInternalChild(
        StoragePageView page, ReadOnlySpan<byte> searchKey,
        Func<StorageTreeKeyReference, byte[]> readKey)
    {
        ArgumentNullException.ThrowIfNull(readKey);
        if (page.Kind != StoragePageKind.Internal) throw new InvalidDataException("Expected B+ tree internal page.");
        ReadOnlySpan<byte> payload = page.Payload.Span;
        int count = ReadHeader(payload, InternalEntry, true);
        ulong firstChild = BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(4, 8));
        if (firstChild == 0) throw new InvalidDataException("Null first child.");
        int lo = 0, hi = count;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int offset = Header + mid * InternalEntry;
            StorageTreeKeyReference reference = ReadKey(payload.Slice(offset, 12));
            byte[] separator = readKey(reference) ?? throw new InvalidDataException("Key reader returned null.");
            if (separator.Length != reference.Length) throw new InvalidDataException("Key length mismatch.");
            if (searchKey.SequenceCompareTo(separator) >= 0) lo = mid + 1;
            else hi = mid;
        }
        ulong child = lo == 0
            ? firstChild
            : BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(Header + (lo - 1) * InternalEntry + 12, 8));
        if (child == 0) throw new InvalidDataException("Null B+ tree child.");
        return child;
    }

    public static StorageBTreeLeafPageView DecodeLeafView(ReadOnlyMemory<byte> bytes,ulong generation,ulong id)
    {
        var page=StoragePageCodec.Decode(bytes,generation,id);
        if(page.Kind!=StoragePageKind.Leaf)throw new InvalidDataException("Expected B+ tree leaf.");
        var payload=page.Payload.Span;int count=ReadHeader(payload,LeafEntry,false);
        ulong next=BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(4,8));
        return new StorageBTreeLeafPageView(page.Payload,next,count);
    }

    public static StorageTreeLeafPage DecodeLeaf(ReadOnlyMemory<byte> bytes,ulong generation,ulong id)
    {
        var page=StoragePageCodec.Decode(bytes,generation,id);
        if(page.Kind!=StoragePageKind.Leaf)throw new InvalidDataException("Expected B+ tree leaf.");
        var p=page.Payload.Span;int n=ReadHeader(p,LeafEntry,false);ulong next=BinaryPrimitives.ReadUInt64LittleEndian(p.Slice(4,8));
        var entries=new StorageTreeLeafEntry[n];int o=Header;
        for(int i=0;i<n;i++)
        {
            var key=ReadKey(p.Slice(o,12));o+=12;ulong vo=BinaryPrimitives.ReadUInt64LittleEndian(p.Slice(o,8));
            int vl=BinaryPrimitives.ReadInt32LittleEndian(p.Slice(o+8,4));byte flags=p[o+12];
            if(vl<0 || flags>1 || (flags==1 && vl!=0))throw new InvalidDataException("Invalid B+ tree value reference.");
            _=checked(vo+(ulong)vl);entries[i]=new(key,new(vo,vl,flags==1));o+=13;
        }
        return new(next,entries);
    }
    public static StorageTreeInternalPage DecodeInternal(ReadOnlyMemory<byte> bytes,ulong generation,ulong id)
        => DecodeInternal(StoragePageCodec.Decode(bytes,generation,id));

    public static StorageTreeInternalPage DecodeInternal(StoragePageView page)
    {
        if(page.Kind!=StoragePageKind.Internal)throw new InvalidDataException("Expected B+ tree internal page.");
        var p=page.Payload.Span;int n=ReadHeader(p,InternalEntry,true);ulong first=BinaryPrimitives.ReadUInt64LittleEndian(p.Slice(4,8));
        if(first==0)throw new InvalidDataException("Null first child.");
        var children=new StorageTreeChild[n];int o=Header;
        for(int i=0;i<n;i++)
        {
            var key=ReadKey(p.Slice(o,12));ulong child=BinaryPrimitives.ReadUInt64LittleEndian(p.Slice(o+12,8));
            if(child==0)throw new InvalidDataException("Null child.");children[i]=new(child,key);o+=InternalEntry;
        }
        return new(first,children);
    }

    public static bool TryFindLeafEntry(ReadOnlyMemory<byte> bytes,ulong generation,ulong id,
        ReadOnlySpan<byte> target,Func<StorageTreeKeyReference,byte[]> readKey,out StorageTreeLeafEntry entry)
    {
        var page=StoragePageCodec.Decode(bytes,generation,id);
        if(page.Kind!=StoragePageKind.Leaf)throw new InvalidDataException("Expected B+ tree leaf.");
        var p=page.Payload.Span;int n=ReadHeader(p,LeafEntry,false);int lo=0,hi=n;
        while(lo<hi)
        {
            int mid=lo+(hi-lo)/2;var candidate=ReadLeafEntry(p,mid);
            byte[] key=readKey(candidate.Key)??throw new InvalidDataException("Key reader returned null.");
            if(key.Length!=candidate.Key.Length)throw new InvalidDataException("Key length mismatch.");
            if(key.AsSpan().SequenceCompareTo(target)<0)lo=mid+1;else hi=mid;
        }
        if(lo<n)
        {
            var candidate=ReadLeafEntry(p,lo);byte[] key=readKey(candidate.Key)??throw new InvalidDataException("Key reader returned null.");
            if(key.Length!=candidate.Key.Length)throw new InvalidDataException("Key length mismatch.");
            if(key.AsSpan().SequenceEqual(target)){entry=candidate;return true;}
        }
        entry=default;return false;
    }

    internal static StorageTreeLeafEntry ReadLeafEntry(ReadOnlySpan<byte> p,int index)
    {
        int o=checked(Header+index*LeafEntry);var key=ReadKey(p.Slice(o,12));o+=12;
        ulong valueOffset=BinaryPrimitives.ReadUInt64LittleEndian(p.Slice(o,8));
        int valueLength=BinaryPrimitives.ReadInt32LittleEndian(p.Slice(o+8,4));byte flags=p[o+12];
        if(valueLength<0 || flags>1 || (flags==1 && valueLength!=0))
            throw new InvalidDataException("Invalid B+ tree value reference.");
        _=checked(valueOffset+(ulong)valueLength);
        return new(key,new(valueOffset,valueLength,flags==1));
    }
    private static int ReadHeader(ReadOnlySpan<byte> p,int size,bool empty)
    {
        if(p.Length<Header || p[0]!=Version || p[1]!=0)throw new InvalidDataException("Invalid B+ tree node header.");
        int n=BinaryPrimitives.ReadUInt16LittleEndian(p.Slice(2,2));
        if((!empty && n==0)||p.Length!=Header+n*size)throw new InvalidDataException("Invalid B+ tree node length.");
        return n;
    }
    private static void WriteKey(Span<byte> p,StorageTreeKeyReference k)
    {
        if(k.Length<=0)throw new InvalidDataException("Invalid key reference.");
        _=checked(k.Offset+(ulong)k.Length);BinaryPrimitives.WriteUInt64LittleEndian(p,k.Offset);
        BinaryPrimitives.WriteInt32LittleEndian(p.Slice(8),k.Length);
    }
    private static StorageTreeKeyReference ReadKey(ReadOnlySpan<byte> p)
    {
        var k=new StorageTreeKeyReference(BinaryPrimitives.ReadUInt64LittleEndian(p),BinaryPrimitives.ReadInt32LittleEndian(p.Slice(8)));
        if(k.Length<=0)throw new InvalidDataException("Invalid key reference.");_=checked(k.Offset+(ulong)k.Length);return k;
    }
}

internal static class StorageBTreeBuilder
{
    public static ulong Build(ulong generation,IEnumerable<StorageTreeBuildEntry> input,Func<ulong> allocate,Action<ulong,byte[]> write)
    {
        if(generation==0)throw new ArgumentOutOfRangeException(nameof(generation));
        ArgumentNullException.ThrowIfNull(input);ArgumentNullException.ThrowIfNull(allocate);ArgumentNullException.ThrowIfNull(write);
        var level=new List<StorageTreeChild>();var leaf=new List<StorageTreeLeafEntry>(StorageBTreePageCodec.MaxLeafEntries);
        byte[]? previous=null;ulong leafId=0;StorageTreeKeyReference first=default;
        foreach(var item in input)
        {
            if(item.Key.IsEmpty)throw new InvalidDataException("Empty key.");
            if(item.ValueLength<0 || (item.IsTombstone && item.ValueLength!=0))throw new InvalidDataException("Invalid value reference.");
            if(previous is not null && previous.AsSpan().SequenceCompareTo(item.Key.Span)>=0)
                throw new InvalidDataException("Input must be strictly sorted and contain no duplicate keys.");
            var key=new StorageTreeKeyReference(item.KeyOffset,item.Key.Length);
            var value=new StorageTreeValueReference(item.ValueOffset,item.ValueLength,item.IsTombstone);
            if(leaf.Count==StorageBTreePageCodec.MaxLeafEntries)
            {
                ulong next=Allocate(allocate);write(leafId,StorageBTreePageCodec.EncodeLeaf(generation,leafId,next,leaf));
                level.Add(new(leafId,first));leafId=next;first=key;leaf.Clear();
            }
            if(leafId==0){leafId=Allocate(allocate);first=key;}
            leaf.Add(new(key,value));previous=item.Key.ToArray();
        }
        if(leaf.Count==0)return 0;
        write(leafId,StorageBTreePageCodec.EncodeLeaf(generation,leafId,0,leaf));level.Add(new(leafId,first));
        while(level.Count>1)
        {
            var upper=new List<StorageTreeChild>((level.Count+StorageBTreePageCodec.MaxInternalChildren-1)/StorageBTreePageCodec.MaxInternalChildren);
            for(int start=0;start<level.Count;start+=StorageBTreePageCodec.MaxInternalChildren)
            {
                int count=Math.Min(StorageBTreePageCodec.MaxInternalChildren,level.Count-start);ulong id=Allocate(allocate);
                var separators=new StorageTreeChild[count-1];for(int i=1;i<count;i++)separators[i-1]=level[start+i];
                write(id,StorageBTreePageCodec.EncodeInternal(generation,id,level[start].PageId,separators));
                upper.Add(new(id,level[start].FirstKey));
            }
            level=upper;
        }
        return level[0].PageId;
    }
    private static ulong Allocate(Func<ulong> allocate){ulong id=allocate();if(id==0)throw new InvalidOperationException("Page ID zero is reserved.");return id;}
}

internal sealed class StorageBTreeReader
{
    private readonly ulong _generation,_root;private readonly Func<ulong,byte[]> _readPage;
    private readonly Func<StorageTreeKeyReference,byte[]> _readKey;
    public StorageBTreeReader(ulong generation,ulong root,Func<ulong,byte[]> readPage,Func<StorageTreeKeyReference,byte[]> readKey)
    {
        if(generation==0)throw new ArgumentOutOfRangeException(nameof(generation));
        _generation=generation;_root=root;_readPage=readPage??throw new ArgumentNullException(nameof(readPage));
        _readKey=readKey??throw new ArgumentNullException(nameof(readKey));
    }
    public bool TryGet(ReadOnlySpan<byte> key,out StorageTreeValueReference value)
    {
        if(key.IsEmpty)throw new ArgumentException("Empty key.",nameof(key));
        if(_root==0){value=default;return false;}
        ulong leafId=FindLeaf(key);
        if(StorageBTreePageCodec.TryFindLeafEntry(_readPage(leafId),_generation,leafId,key,_readKey,out var entry)
            && !entry.Value.IsTombstone){value=entry.Value;return true;}
        value=default;return false;
    }
    public StorageBTreePrefixCursor OpenPrefixCursor(ReadOnlyMemory<byte> prefix)
    {
        if(prefix.IsEmpty)throw new ArgumentException("Empty prefix.",nameof(prefix));
        if(_root==0)return new(this,0,0,prefix.ToArray());
        ulong id=FindLeaf(prefix.Span);var leaf=ReadLeafView(id);return new(this,id,LowerBound(leaf,prefix.Span),prefix.ToArray(),leaf);
    }
    internal StorageBTreeLeafPageView ReadLeafView(ulong id)
        => StorageBTreePageCodec.DecodeLeafView(_readPage(id),_generation,id);
    internal byte[] ReadKey(StorageTreeKeyReference r){byte[] key=_readKey(r)??throw new InvalidDataException("Key reader returned null.");if(key.Length!=r.Length)throw new InvalidDataException("Key length mismatch.");return key;}
    internal bool HasPrefix(ReadOnlySpan<byte> key,ReadOnlySpan<byte> prefix)=>key.StartsWith(prefix);
    private ulong FindLeaf(ReadOnlySpan<byte> key)
    {
        ulong id=_root;
        for(int depth=0;depth<64;depth++)
        {
            byte[] bytes=_readPage(id)??throw new InvalidDataException("Page reader returned null.");
            var view=StoragePageCodec.Decode(bytes,_generation,id);
            if(view.Kind==StoragePageKind.Leaf)return id;
            id=StorageBTreePageCodec.SelectInternalChild(view,key,ReadKey);
        }
        throw new InvalidDataException("B+ tree depth exceeds the supported maximum or contains a cycle.");
    }
    private int LowerBound(StorageBTreeLeafPageView leaf,ReadOnlySpan<byte> key)
    {
        int lo=0,hi=leaf.Count;
        while(lo<hi)
        {
            int mid=lo+(hi-lo)/2;
            if(ReadKey(leaf.GetEntry(mid).Key).AsSpan().SequenceCompareTo(key)<0)lo=mid+1;
            else hi=mid;
        }
        return lo;
    }
}

internal sealed class StorageBTreePrefixCursor:IDisposable
{
    private readonly StorageBTreeReader _reader;private readonly byte[] _prefix;
    private ulong _page;private int _index;private bool _entered,_finished,_disposed;private byte[]? _lastKey;
    private StorageBTreeLeafPageView? _leafPage;
    internal StorageBTreePrefixCursor(StorageBTreeReader reader,ulong page,int index,byte[] prefix,StorageBTreeLeafPageView? initialLeaf=null)
    {
        _reader=reader;_page=page;_index=index;_prefix=prefix;_finished=page==0;
        _leafPage=initialLeaf;_entered=initialLeaf.HasValue;
    }
    public bool TryReadNext(out StorageTreeEntry entry)
    {
        ObjectDisposedException.ThrowIf(_disposed,this);
        while(!_finished)
        {
            if(!_entered)
            {
                _leafPage=_reader.ReadLeafView(_page);_entered=true;
            }
            var leaf=_leafPage!.Value;
            while(_index<leaf.Count)
            {
                var e=leaf.GetEntry(_index++);byte[] key=_reader.ReadKey(e.Key);
                if(_lastKey is not null&&_lastKey.AsSpan().SequenceCompareTo(key)>=0)throw new InvalidDataException("Leaf chain ordering error.");
                _lastKey=key;if(!_reader.HasPrefix(key,_prefix)){_finished=true;break;}
                if(!e.Value.IsTombstone){entry=new(e.Key,e.Value);return true;}
            }
            if(_finished||leaf.NextPageId==0){_finished=true;break;}
            _page=leaf.NextPageId;_index=0;_entered=false;_leafPage=null;
        }
        entry=default;return false;
    }
    public void Dispose(){_disposed=true;_finished=true;_leafPage=null;}
}
