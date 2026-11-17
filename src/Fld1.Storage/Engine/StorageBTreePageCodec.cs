using System.Buffers.Binary;
namespace Fld1.Storage.Engine;

internal readonly record struct StorageTreeKeyReference(ulong Offset,int Length);
internal readonly record struct StorageTreeValueReference(ulong Offset,int Length,bool IsTombstone);
internal readonly record struct StorageTreeEntry(StorageTreeKeyReference Key,StorageTreeValueReference Value);
internal readonly record struct StorageTreeBuildEntry(ReadOnlyMemory<byte> Key,ulong KeyOffset,ulong ValueOffset,int ValueLength,bool IsTombstone);
internal readonly record struct StorageTreeLeafEntry(StorageTreeKeyReference Key,StorageTreeValueReference Value);
internal readonly record struct StorageTreeChild(ulong PageId,StorageTreeKeyReference FirstKey);
internal readonly record struct StorageTreePathFrame(ulong PageId, int ChildIndex);
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
        Func<StorageTreeKeyReference, ReadOnlyMemory<byte>> readKey)
        => SelectInternalChild(StoragePageCodec.Decode(bytes, generation, pageId), searchKey, readKey);

    public static ulong SelectInternalChild(
        StoragePageView page, ReadOnlySpan<byte> searchKey,
        Func<StorageTreeKeyReference, ReadOnlyMemory<byte>> readKey)
    {
        int index = SelectInternalChildIndex(page, searchKey, readKey);
        ReadOnlySpan<byte> payload = page.Payload.Span;
        ulong child = index == 0
            ? BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(4, 8))
            : BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(Header + (index - 1) * InternalEntry + 12, 8));
        if (child == 0) throw new InvalidDataException("Null B+ tree child.");
        return child;
    }

    public static int SelectInternalChildIndex(
        StoragePageView page, ReadOnlySpan<byte> searchKey,
        Func<StorageTreeKeyReference, ReadOnlyMemory<byte>> readKey)
    {
        ArgumentNullException.ThrowIfNull(readKey);
        if (page.Kind != StoragePageKind.Internal) throw new InvalidDataException("Expected B+ tree internal page.");
        ReadOnlySpan<byte> payload = page.Payload.Span;
        int count = ReadHeader(payload, InternalEntry, true);
        if (BinaryPrimitives.ReadUInt64LittleEndian(payload.Slice(4, 8)) == 0)
            throw new InvalidDataException("Null first child.");
        int lo = 0, hi = count;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            int offset = Header + mid * InternalEntry;
            StorageTreeKeyReference reference = ReadKey(payload.Slice(offset, 12));
            ReadOnlyMemory<byte> separator = readKey(reference);
            if (separator.Length != reference.Length) throw new InvalidDataException("Key length mismatch.");
            if (searchKey.SequenceCompareTo(separator.Span) >= 0) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    public static ulong GetInternalChild(StorageTreeInternalPage node, int childIndex)
    {
        ArgumentNullException.ThrowIfNull(node);
        if ((uint)childIndex > (uint)node.Separators.Length)
            throw new ArgumentOutOfRangeException(nameof(childIndex));
        return childIndex == 0 ? node.FirstChild : node.Separators[childIndex - 1].PageId;
    }

    public static StorageBTreeLeafPageView DecodeLeafView(ReadOnlyMemory<byte> bytes,ulong generation,ulong id)
        => DecodeLeafView(StoragePageCodec.Decode(bytes,generation,id));

    public static StorageBTreeLeafPageView DecodeLeafView(StoragePageView page)
    {
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
        ReadOnlySpan<byte> target,Func<StorageTreeKeyReference,ReadOnlyMemory<byte>> readKey,out StorageTreeLeafEntry entry)
        => TryFindLeafEntry(DecodeLeafView(bytes,generation,id),target,readKey,out entry);

    public static bool TryFindLeafEntry(StorageBTreeLeafPageView leaf,
        ReadOnlySpan<byte> target,Func<StorageTreeKeyReference,ReadOnlyMemory<byte>> readKey,out StorageTreeLeafEntry entry)
    {
        ArgumentNullException.ThrowIfNull(readKey);
        int lo=0,hi=leaf.Count;
        while(lo<hi)
        {
            int mid=lo+(hi-lo)/2;var candidate=leaf.GetEntry(mid);
            ReadOnlyMemory<byte> key=readKey(candidate.Key);
            if(key.Length!=candidate.Key.Length)throw new InvalidDataException("Key length mismatch.");
            if(key.Span.SequenceCompareTo(target)<0)lo=mid+1;else hi=mid;
        }
        if(lo<leaf.Count)
        {
            var candidate=leaf.GetEntry(lo);ReadOnlyMemory<byte> key=readKey(candidate.Key);
            if(key.Length!=candidate.Key.Length)throw new InvalidDataException("Key length mismatch.");
            if(key.Span.SequenceEqual(target)){entry=candidate;return true;}
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
        // Input key memory is borrowed: it must remain immutable until Build returns.
        ReadOnlyMemory<byte>? previous=null;ulong leafId=0;StorageTreeKeyReference first=default;
        foreach(var item in input)
        {
            if(item.Key.IsEmpty)throw new InvalidDataException("Empty key.");
            if(item.ValueLength<0 || (item.IsTombstone && item.ValueLength!=0))throw new InvalidDataException("Invalid value reference.");
            if(previous.HasValue && previous.Value.Span.SequenceCompareTo(item.Key.Span)>=0)
                throw new InvalidDataException("Input must be strictly sorted and contain no duplicate keys.");
            var key=new StorageTreeKeyReference(item.KeyOffset,item.Key.Length);
            var value=new StorageTreeValueReference(item.ValueOffset,item.ValueLength,item.IsTombstone);
            if(leaf.Count==StorageBTreePageCodec.MaxLeafEntries)
            {
                ulong next=Allocate(allocate);write(leafId,StorageBTreePageCodec.EncodeLeaf(generation,leafId,next,leaf));
                level.Add(new(leafId,first));leafId=next;first=key;leaf.Clear();
            }
            if(leafId==0){leafId=Allocate(allocate);first=key;}
            leaf.Add(new(key,value));previous=item.Key;
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
    private const int ValidationCacheCapacity = 4096;
    private const int HotLookupCapacity = 16_384;
    private const int HotLookupMaxKeyLength = 256;
    private readonly object _hotGate = new();
    private readonly Dictionary<ulong, HotLookupEntry> _hotEntries = new();
    private readonly Queue<ulong> _hotOrder = new();
    private readonly ulong _generation,_root;private readonly Func<ulong,byte[]>? _readPage;
    private readonly Func<ulong,StoragePageCacheLease>? _readPageLease;
    private readonly Func<StorageTreeKeyReference,ReadOnlyMemory<byte>> _readKey;
    private readonly object _validationGate = new();
    private readonly HashSet<ulong> _validatedPages = new();
    private readonly Queue<ulong> _validationOrder = new();
    public StorageBTreeReader(ulong generation,ulong root,Func<ulong,byte[]> readPage,Func<StorageTreeKeyReference,ReadOnlyMemory<byte>> readKey)
    {
        if(generation==0)throw new ArgumentOutOfRangeException(nameof(generation));
        _generation=generation;_root=root;_readPage=readPage??throw new ArgumentNullException(nameof(readPage));
        _readKey=readKey??throw new ArgumentNullException(nameof(readKey));
    }
    public StorageBTreeReader(ulong generation,ulong root,Func<ulong,StoragePageCacheLease> readPageLease,
        Func<StorageTreeKeyReference,ReadOnlyMemory<byte>> readKey)
    {
        if(generation==0)throw new ArgumentOutOfRangeException(nameof(generation));
        _generation=generation;_root=root;_readPageLease=readPageLease??throw new ArgumentNullException(nameof(readPageLease));
        _readKey=readKey??throw new ArgumentNullException(nameof(readKey));
    }
    public bool TryGet(ReadOnlySpan<byte> key,out StorageTreeValueReference value)
    {
        if(key.IsEmpty)throw new ArgumentException("Empty key.",nameof(key));
        if(TryGetHot(key,out bool cachedFound,out value))return cachedFound;
        if(_root==0){value=default;RememberHot(key,false,default);return false;}
        ulong leafId=FindLeaf(key);
        var leaf=ReadLeafView(leafId,out var lease);
        try
        {
            if(StorageBTreePageCodec.TryFindLeafEntry(leaf,key,_readKey,out var entry)
                && !entry.Value.IsTombstone)
            {
                value=entry.Value;RememberHot(key,true,value);return true;
            }
            value=default;RememberHot(key,false,default);return false;
        }
        finally { lease?.Dispose(); }
    }

    internal int HotLookupCount { get { lock(_hotGate)return _hotEntries.Count; } }

    private bool TryGetHot(ReadOnlySpan<byte> key,out bool found,out StorageTreeValueReference value)
    {
        if(key.Length>HotLookupMaxKeyLength){found=false;value=default;return false;}
        ulong hash=HashKey(key);
        lock(_hotGate)
        {
            if(_hotEntries.TryGetValue(hash,out HotLookupEntry entry)
                && entry.Key.AsSpan().SequenceEqual(key))
            {
                found=entry.Found;value=entry.Value;return true;
            }
        }
        found=false;value=default;return false;
    }

    private void RememberHot(ReadOnlySpan<byte> key,bool found,StorageTreeValueReference value)
    {
        if(key.Length>HotLookupMaxKeyLength)return;
        ulong hash=HashKey(key);
        lock(_hotGate)
        {
            // Hash collisions only reduce cache coverage; they never affect lookup correctness.
            if(_hotEntries.ContainsKey(hash))return;
            if(_hotEntries.Count>=HotLookupCapacity)
                _hotEntries.Remove(_hotOrder.Dequeue());
            _hotEntries.Add(hash,new HotLookupEntry(key.ToArray(),found,value));
            _hotOrder.Enqueue(hash);
        }
    }

    private static ulong HashKey(ReadOnlySpan<byte> key)
    {
        const ulong offset=14695981039346656037UL;
        const ulong prime=1099511628211UL;
        ulong hash=offset;
        foreach(byte value in key){hash^=value;hash*=prime;}
        return hash^checked((ulong)key.Length);
    }

    private readonly record struct HotLookupEntry(byte[] Key,bool Found,StorageTreeValueReference Value);
    public StorageBTreePrefixCursor OpenPrefixCursor(ReadOnlyMemory<byte> prefix)
    {
        if(prefix.IsEmpty)throw new ArgumentException("Empty prefix.",nameof(prefix));
        var path = new List<StorageTreePathFrame>();
        if(_root==0)return new(this,0,0,prefix.ToArray(),path);
        ulong id=FindLeaf(prefix.Span,path);var leaf=ReadLeafView(id,out var lease);
        try { return new(this,id,LowerBound(leaf,prefix.Span),prefix.ToArray(),path,leaf,lease); }
        catch { lease?.Dispose();throw; }
    }
    internal StorageBTreeLeafPageView ReadLeafView(ulong id,out StoragePageCacheLease? lease)
    {
        lease=_readPageLease?.Invoke(id);
        try
        {
            ReadOnlyMemory<byte> bytes=lease is not null
                ? lease.Page
                : _readPage!(id)??throw new InvalidDataException("Page reader returned null.");
            StoragePageView page = lease is not null
                ? lease.Decode(_generation, id)
                : StoragePageCodec.Decode(bytes, _generation, id);
            var leaf=StorageBTreePageCodec.DecodeLeafView(page);
            bool validated = IsValidated(id);
            if(!validated)
            {
                ReadOnlyMemory<byte>? previous=null;
                for(int i=0;i<leaf.Count;i++)
                {
                    var entry=leaf.GetEntry(i);
                    ReadOnlyMemory<byte> current=ReadKey(entry.Key);
                    if(previous.HasValue && previous.Value.Span.SequenceCompareTo(current.Span)>=0)
                        throw new InvalidDataException("B+ tree leaf keys are not strictly ordered.");
                    previous=current;
                }
                MarkValidated(id);
            }
            return leaf;
        }
        catch { lease?.Dispose();lease=null;throw; }
    }
    internal ReadOnlyMemory<byte> ReadKey(StorageTreeKeyReference r){ReadOnlyMemory<byte> key=_readKey(r);if(key.Length!=r.Length)throw new InvalidDataException("Key length mismatch.");return key;}
    internal bool HasPrefix(StorageTreeKeyReference key,ReadOnlySpan<byte> prefix)=>ReadKey(key).Span.StartsWith(prefix);
    internal int CompareKeys(StorageTreeKeyReference left,StorageTreeKeyReference right)
        => ReadKey(left).Span.SequenceCompareTo(ReadKey(right).Span);
    private ulong FindLeaf(ReadOnlySpan<byte> key) => FindLeafCore(key, path: null);

    private ulong FindLeaf(ReadOnlySpan<byte> key, List<StorageTreePathFrame> path)
        => FindLeafCore(key, path ?? throw new ArgumentNullException(nameof(path)));

    private ulong FindLeafCore(ReadOnlySpan<byte> key, List<StorageTreePathFrame>? path)
    {
        ulong id=_root;
        for(int depth=0;depth<64;depth++)
        {
            StoragePageCacheLease? lease=_readPageLease?.Invoke(id);
            try
            {
                ReadOnlyMemory<byte> bytes=lease is not null
                    ? lease.Page
                    : _readPage!(id)??throw new InvalidDataException("Page reader returned null.");
                var view = lease is not null
                    ? lease.Decode(_generation, id)
                    : StoragePageCodec.Decode(bytes, _generation, id);
                if(view.Kind==StoragePageKind.Leaf)return id;
                if(path is null)
                {
                    if(!IsValidated(id))
                    {
                        var node=StorageBTreePageCodec.DecodeInternal(view);
                        ValidateInternalNode(id,node);
                    }
                    id=StorageBTreePageCodec.SelectInternalChild(view,key,ReadKey);
                }
                else
                {
                    var node=StorageBTreePageCodec.DecodeInternal(view);
                    ValidateInternalNode(id,node);
                    int childIndex=StorageBTreePageCodec.SelectInternalChildIndex(view,key,ReadKey);
                    path.Add(new StorageTreePathFrame(id,childIndex));
                    id=StorageBTreePageCodec.GetInternalChild(node,childIndex);
                }
            }
            finally { lease?.Dispose(); }
        }
        throw new InvalidDataException("B+ tree depth exceeds the supported maximum or contains a cycle.");
    }

    private void ValidateInternalNode(ulong id, StorageTreeInternalPage node)
    {
        if(IsValidated(id))return;
        var children=new HashSet<ulong>{node.FirstChild};
        StorageTreeKeyReference? previous=null;
        foreach(var separator in node.Separators)
        {
            if(previous.HasValue && CompareKeys(previous.Value,separator.FirstKey)>=0)
                throw new InvalidDataException("B+ tree internal separators are not strictly ordered.");
            if(!children.Add(separator.PageId))
                throw new InvalidDataException("B+ tree internal page contains a duplicate child reference.");
            previous=separator.FirstKey;
        }
        MarkValidated(id);
    }

    private StoragePageView ReadPageView(ulong id, out StoragePageCacheLease? lease)
    {
        lease=_readPageLease?.Invoke(id);
        try
        {
            ReadOnlyMemory<byte> bytes=lease is not null
                ? lease.Page
                : _readPage!(id)??throw new InvalidDataException("Page reader returned null.");
            return lease is not null
                ? lease.Decode(_generation,id)
                : StoragePageCodec.Decode(bytes,_generation,id);
        }
        catch { lease?.Dispose();lease=null;throw; }
    }

    private StorageTreeInternalPage ReadInternalNode(ulong id)
    {
        var page=ReadPageView(id,out var lease);
        try
        {
            var node=StorageBTreePageCodec.DecodeInternal(page);
            ValidateInternalNode(id,node);
            return node;
        }
        finally { lease?.Dispose(); }
    }

    internal bool TryAdvanceToNextLeaf(List<StorageTreePathFrame> path, out ulong leafId)
    {
        for(int i=path.Count-1;i>=0;i--)
        {
            StorageTreePathFrame frame=path[i];
            StorageTreeInternalPage node=ReadInternalNode(frame.PageId);
            int nextChild=frame.ChildIndex+1;
            if(nextChild>=node.Separators.Length+1)continue;

            path[i]=new StorageTreePathFrame(frame.PageId,nextChild);
            if(path.Count>i+1)path.RemoveRange(i+1,path.Count-i-1);
            ulong id=StorageBTreePageCodec.GetInternalChild(node,nextChild);

            for(int depth=0;depth<64;depth++)
            {
                var page=ReadPageView(id,out var lease);
                try
                {
                    if(page.Kind==StoragePageKind.Leaf){leafId=id;return true;}
                    var childNode=StorageBTreePageCodec.DecodeInternal(page);
                    ValidateInternalNode(id,childNode);
                    path.Add(new StorageTreePathFrame(id,0));
                    id=childNode.FirstChild;
                }
                finally { lease?.Dispose(); }
            }
            throw new InvalidDataException("B+ tree depth exceeds the supported maximum or contains a cycle.");
        }
        leafId=0;
        return false;
    }
    private bool IsValidated(ulong id){lock(_validationGate)return _validatedPages.Contains(id);}
    private void MarkValidated(ulong id)
    {
        lock(_validationGate)
        {
            if(!_validatedPages.Add(id))return;
            _validationOrder.Enqueue(id);
            while(_validationOrder.Count>ValidationCacheCapacity)
                _validatedPages.Remove(_validationOrder.Dequeue());
        }
    }
    private int LowerBound(StorageBTreeLeafPageView leaf,ReadOnlySpan<byte> key)
    {
        int lo=0,hi=leaf.Count;
        while(lo<hi)
        {
            int mid=lo+(hi-lo)/2;
            if(ReadKey(leaf.GetEntry(mid).Key).Span.SequenceCompareTo(key)<0)lo=mid+1;
            else hi=mid;
        }
        return lo;
    }
}

internal sealed class StorageBTreePrefixCursor:IDisposable
{
    private readonly StorageBTreeReader _reader;private readonly byte[] _prefix;
    private readonly List<StorageTreePathFrame> _path;
    private ulong _page;private int _index;private bool _entered,_finished,_disposed,_leafFullyMatches;private StorageTreeKeyReference? _previousLeafLastKey;
    private StorageBTreeLeafPageView? _leafPage;
    private StoragePageCacheLease? _leafLease;
    internal StorageBTreePrefixCursor(StorageBTreeReader reader,ulong page,int index,byte[] prefix,
        List<StorageTreePathFrame> path,StorageBTreeLeafPageView? initialLeaf=null,StoragePageCacheLease? initialLease=null)
    {
        _reader=reader;_page=page;_index=index;_prefix=prefix;_path=path;_finished=page==0;
        _leafPage=initialLeaf;_leafLease=initialLease;_entered=initialLeaf.HasValue;
    }
    public bool TryReadNext(out StorageTreeEntry entry)
    {
        ObjectDisposedException.ThrowIf(_disposed,this);
        while(!_finished)
        {
            if(!_entered)
            {
                _leafPage=_reader.ReadLeafView(_page,out _leafLease);
                var loadedLeaf=_leafPage.Value;
                if(_previousLeafLastKey.HasValue && loadedLeaf.Count>0
                    && _reader.CompareKeys(_previousLeafLastKey.Value,loadedLeaf.GetEntry(0).Key)>=0)
                    throw new InvalidDataException("Leaf chain ordering error.");
                _leafFullyMatches = _index < loadedLeaf.Count
                    && _reader.HasPrefix(loadedLeaf.GetEntry(_index).Key,_prefix)
                    && _reader.HasPrefix(loadedLeaf.GetEntry(loadedLeaf.Count-1).Key,_prefix);
                _entered=true;
            }
            var leaf=_leafPage!.Value;
            while(_index<leaf.Count)
            {
                var e=leaf.GetEntry(_index++);
                if(!_leafFullyMatches&&!_reader.HasPrefix(e.Key,_prefix)){_finished=true;break;}
                if(!e.Value.IsTombstone){entry=new(e.Key,e.Value);return true;}
            }
            if(leaf.Count>0)_previousLeafLastKey=leaf.GetEntry(leaf.Count-1).Key;
            _leafLease?.Dispose();_leafLease=null;
            bool hasNext=_reader.TryAdvanceToNextLeaf(_path,out ulong nextPage);
            // The internal tree is authoritative. COW leaf pages are immutable and
            // may have stale legacy NextPageId hints on their unchanged neighbors.
            if(!hasNext){_finished=true;break;}
            _page=nextPage;_index=0;_entered=false;_leafPage=null;
        }
        entry=default;return false;
    }
    public void Dispose(){_disposed=true;_finished=true;_leafPage=null;_leafLease?.Dispose();_leafLease=null;}
}
