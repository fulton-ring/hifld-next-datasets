import urllib.request,urllib.error,pathlib,json,datetime,hashlib,sys
ROOT=pathlib.Path(__file__).resolve().parent
def get(name,url):
 try:
  with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'HIFLD-Next-source-verification/1.0'}),timeout=90) as r: b=r.read(); status=r.status;final=r.url;headers=dict(r.headers)
 except urllib.error.HTTPError as e:b=e.read();status=e.code;final=e.url;headers=dict(e.headers)
 except Exception as e:
  print(name,type(e).__name__,str(e));return
 ROOT.joinpath(name+'.body').write_bytes(b)
 d={'requested_url':url,'final_url':final,'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':status,'byte_length':len(b),'sha256':hashlib.sha256(b).hexdigest(),'headers':headers}
 ROOT.joinpath(name+'.receipt.json').write_text(json.dumps(d,indent=2));print(name,status,len(b))
if __name__=='__main__':get(sys.argv[1],sys.argv[2])
