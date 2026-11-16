using Fld1.Storage.Engine;
using Xunit;
namespace Fld1.Storage.Tests;
public sealed class StorageBTreeTests
{
    [Fact]
    public void Randomized_builder_reader_prefix_and_tombstones_match_reference()
    {
        var random=new Random(731991);var reference=new SortedDictionary<string,(int Value,bool Deleted)>(StringComparer.Ordinal);
        for(int i=0;i<15_000;i++){string k=$"{random.Next(0,9000):D5}-{random.Next(0,100):D2}";reference[k]=(i,i%13==0);}
        var pages=new Dictionary<ulong,byte[]>();var keys=new Dictionary<ulong,byte[]>();ulong np=1,nk=1,nv=1;
        var input=new List<StorageTreeBuildEntry>(reference.Count);
        foreach(var pair in reference){byte[] k=System.Text.Encoding.UTF8.GetBytes(pair.Key);ulong off=nk++;keys.Add(off,k);input.Add(new(k,off,nv++,pair.Value.Deleted?0:4,pair.Value.Deleted));}
        ulong root=StorageBTreeBuilder.Build(7,input,()=>np++,(id,p)=>Assert.True(pages.TryAdd(id,p)));
        Assert.NotEqual(0UL,root);Assert.True(pages.Count>20);
        var reader=new StorageBTreeReader(7,root,id=>pages[id],r=>keys[r.Offset]);
        int sampled=0;foreach(var pair in reference){if((sampled++%17)!=0)continue;byte[] k=System.Text.Encoding.UTF8.GetBytes(pair.Key);bool found=reader.TryGet(k,out var v);Assert.Equal(!pair.Value.Deleted,found);if(found)Assert.Equal(4,v.Length);}
        const string prefix="00042-";using var cursor=reader.OpenPrefixCursor(System.Text.Encoding.UTF8.GetBytes(prefix));var actual=new List<string>();
        while(cursor.TryReadNext(out var e))actual.Add(System.Text.Encoding.UTF8.GetString(keys[e.Key.Offset]));
        string[] expected=reference.Where(x=>x.Key.StartsWith(prefix,StringComparison.Ordinal)&&!x.Value.Deleted).Select(x=>x.Key).ToArray();
        Assert.Equal(expected,actual);
    }
    [Fact]
    public void Prefix_cursor_reads_each_leaf_once_and_uses_constant_cursor_state()
    {
        var entries=new List<StorageTreeBuildEntry>();var keys=new Dictionary<ulong,byte[]>();ulong offset=1;
        for(int i=0;i<1400;i++)
        {
            byte[] key=new byte[4];System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(key,i);
            keys.Add(offset,key);entries.Add(new(key,offset,offset,1,false));offset++;
        }
        var pages=new Dictionary<ulong,byte[]>();ulong next=1;
        ulong root=StorageBTreeBuilder.Build(4,entries,()=>next++,(id,page)=>pages.Add(id,page));
        int reads=0;var reader=new StorageBTreeReader(4,root,id=>{reads++;return pages[id];},r=>keys[r.Offset]);
        using var cursor=reader.OpenPrefixCursor(new byte[]{0,0});
        Assert.True(cursor.TryReadNext(out _));int afterFirst=reads;
        Assert.True(cursor.TryReadNext(out _));Assert.Equal(afterFirst,reads);
        int total=2;while(cursor.TryReadNext(out _))total++;
        Assert.Equal(1400,total);Assert.True(reads>=3);
    }

    [Fact]
    public void Prefix_cursor_detects_leaf_cycle_by_strict_key_order()
    {
        byte[] k1={0,1},k2={0,2};var keys=new Dictionary<ulong,byte[]>{{1,k1},{2,k2}};
        var entries=new[]{new StorageTreeBuildEntry(k1,1,1,1,false),new StorageTreeBuildEntry(k2,2,2,1,false)};
        var pages=new Dictionary<ulong,byte[]>();ulong next=1;
        ulong root=StorageBTreeBuilder.Build(5,entries,()=>next++,(id,page)=>pages.Add(id,page));
        pages[1]=StorageBTreePageCodec.EncodeLeaf(5,1,1,new[]{
            new StorageTreeLeafEntry(new(1,2),new(1,1,false)),
            new StorageTreeLeafEntry(new(2,2),new(2,1,false))});
        var reader=new StorageBTreeReader(5,root,id=>pages[id],r=>keys[r.Offset]);
        using var cursor=reader.OpenPrefixCursor(new byte[]{0});
        Assert.True(cursor.TryReadNext(out _));Assert.True(cursor.TryReadNext(out _));
        Assert.Throws<InvalidDataException>(()=>cursor.TryReadNext(out _));
    }


    [Fact]
    public void Reader_rejects_crc_valid_leaf_with_unsorted_keys()
    {
        byte[] first={0x01},second={0x02};
        var pages=new Dictionary<ulong,byte[]>
        {
            [1]=StorageBTreePageCodec.EncodeLeaf(9,1,0,new[]
            {
                new StorageTreeLeafEntry(new(2,1),new(2,1,false)),
                new StorageTreeLeafEntry(new(1,1),new(1,1,false))
            })
        };
        var keys=new Dictionary<ulong,byte[]>{{1,first},{2,second}};
        var reader=new StorageBTreeReader(9,1,id=>pages[id],r=>keys[r.Offset]);
        Assert.Throws<InvalidDataException>(()=>reader.TryGet(first,out _));
        Assert.Throws<InvalidDataException>(()=>
        {
            using var cursor=reader.OpenPrefixCursor(first);
            cursor.TryReadNext(out _);
        });
    }

    [Fact]
    public void Reader_rejects_crc_valid_internal_page_with_unsorted_separators()
    {
        byte[] one={0x01},two={0x02},three={0x03};
        var pages=new Dictionary<ulong,byte[]>
        {
            [1]=StorageBTreePageCodec.EncodeLeaf(10,1,0,new[]
                {new StorageTreeLeafEntry(new(1,1),new(1,1,false))}),
            [2]=StorageBTreePageCodec.EncodeLeaf(10,2,0,new[]
                {new StorageTreeLeafEntry(new(2,1),new(2,1,false))}),
            [3]=StorageBTreePageCodec.EncodeInternal(10,3,1,new[]
            {
                new StorageTreeChild(2,new(2,1)),
                new StorageTreeChild(4,new(3,1))
            })
        };
        var keys=new Dictionary<ulong,byte[]>{{1,one},{2,two},{3,one}};
        var reader=new StorageBTreeReader(10,3,id=>pages[id],r=>keys[r.Offset]);
        Assert.Throws<InvalidDataException>(()=>reader.TryGet(one,out _));
    }

    [Fact] public void Builder_rejects_duplicate_and_unsorted_keys()
    {
        var duplicate=new[]{new StorageTreeBuildEntry(new byte[]{1},1,1,1,false),new StorageTreeBuildEntry(new byte[]{1},2,2,1,false)};
        var unsorted=new[]{new StorageTreeBuildEntry(new byte[]{2},1,1,1,false),new StorageTreeBuildEntry(new byte[]{1},2,2,1,false)};
        ulong id=1;
        Assert.Throws<InvalidDataException>(()=>StorageBTreeBuilder.Build(1,duplicate,()=>id++,(_,_)=>{}));
        Assert.Throws<InvalidDataException>(()=>StorageBTreeBuilder.Build(1,unsorted,()=>id++,(_,_)=>{}));
    }
    [Fact] public void Empty_tree_and_one_megabyte_external_key_work(){ulong id=1;Assert.Equal(0UL,StorageBTreeBuilder.Build(1,Array.Empty<StorageTreeBuildEntry>(),()=>id++,(_,_)=>{}));byte[] k=new byte[1024*1024];k[^1]=1;var pages=new Dictionary<ulong,byte[]>();ulong p=1;ulong root=StorageBTreeBuilder.Build(2,new[]{new StorageTreeBuildEntry(k,99,100,3,false)},()=>p++,(n,b)=>pages.Add(n,b));var reader=new StorageBTreeReader(2,root,n=>pages[n],_=>k);Assert.True(reader.TryGet(k,out var v));Assert.Equal(3,v.Length);}
    [Fact] public void Page_codec_rejects_identity_and_crc_corruption(){byte[] p=StorageBTreePageCodec.EncodeLeaf(3,4,0,new[]{new StorageTreeLeafEntry(new(1,2),new(10,5,false))});Assert.Throws<InvalidDataException>(()=>StorageBTreePageCodec.DecodeLeaf(p,3,5));p[^1]=0x7f;Assert.Throws<InvalidDataException>(()=>StorageBTreePageCodec.DecodeLeaf(p,3,4));}
}
