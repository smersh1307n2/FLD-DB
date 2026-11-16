using Fld1.Storage.Engine;
using Xunit;
namespace Fld1.Storage.Tests;
public sealed class StorageBTreeTests
{
    [Fact]
    public void Randomized_builder_reader_prefix_and_tombstones_match_reference()
    {
        var random=new Random(731991);var reference=new SortedDictionary<string,(int Value,bool Deleted)>(StringComparer.Ordinal);
        for(int i=0;i<30_000;i++){string k=$"{random.Next(0,9000):D5}-{random.Next(0,100):D2}";reference[k]=(i,i%13==0);}
        var pages=new Dictionary<ulong,byte[]>();var keys=new Dictionary<ulong,byte[]>();ulong np=1,nk=1,nv=1;
        var input=new List<StorageTreeBuildEntry>(reference.Count);
        foreach(var pair in reference){byte[] k=System.Text.Encoding.UTF8.GetBytes(pair.Key);ulong off=nk++;keys.Add(off,k);input.Add(new(k,off,nv++,pair.Value.Deleted?0:4,pair.Value.Deleted));}
        ulong root=StorageBTreeBuilder.Build(7,input,()=>np++,(id,p)=>Assert.True(pages.TryAdd(id,p)));
        Assert.NotEqual(0UL,root);Assert.True(pages.Count>20);
        var reader=new StorageBTreeReader(7,root,id=>pages[id],r=>keys[r.Offset]);
        foreach(var pair in reference){byte[] k=System.Text.Encoding.UTF8.GetBytes(pair.Key);bool found=reader.TryGet(k,out var v);Assert.Equal(!pair.Value.Deleted,found);if(found)Assert.Equal(4,v.Length);}
        const string prefix="00042-";using var cursor=reader.OpenPrefixCursor(System.Text.Encoding.UTF8.GetBytes(prefix));var actual=new List<string>();
        while(cursor.TryReadNext(out var e))actual.Add(System.Text.Encoding.UTF8.GetString(keys[e.Key.Offset]));
        string[] expected=reference.Where(x=>x.Key.StartsWith(prefix,StringComparison.Ordinal)&&!x.Value.Deleted).Select(x=>x.Key).ToArray();
        Assert.Equal(expected,actual);
    }
    [Fact] public void Builder_rejects_duplicate_keys(){var a=new[]{new StorageTreeBuildEntry(new byte[]{1},1,1,1,false),new StorageTreeBuildEntry(new byte[]{1},2,2,1,false)};ulong id=1;Assert.Throws<InvalidDataException>(()=>StorageBTreeBuilder.Build(1,a,()=>id++,(_,_)=>{}));}
    [Fact] public void Empty_tree_and_one_megabyte_external_key_work(){ulong id=1;Assert.Equal(0UL,StorageBTreeBuilder.Build(1,Array.Empty<StorageTreeBuildEntry>(),()=>id++,(_,_)=>{}));byte[] k=new byte[1024*1024];k[^1]=1;var pages=new Dictionary<ulong,byte[]>();ulong p=1;ulong root=StorageBTreeBuilder.Build(2,new[]{new StorageTreeBuildEntry(k,99,100,3,false)},()=>p++,(n,b)=>pages.Add(n,b));var reader=new StorageBTreeReader(2,root,n=>pages[n],_=>k);Assert.True(reader.TryGet(k,out var v));Assert.Equal(3,v.Length);}
    [Fact] public void Page_codec_rejects_identity_and_crc_corruption(){byte[] p=StorageBTreePageCodec.EncodeLeaf(3,4,0,new[]{new StorageTreeLeafEntry(new(1,2),new(10,5,false))});Assert.Throws<InvalidDataException>(()=>StorageBTreePageCodec.DecodeLeaf(p,3,5));p[^1]=0x7f;Assert.Throws<InvalidDataException>(()=>StorageBTreePageCodec.DecodeLeaf(p,3,4));}
}
