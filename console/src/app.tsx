import { useCallback, useEffect, useState } from 'react'
import { api } from './api'
import { Snapshot } from './features'
import { Overview } from './features/overview'
import { Runtime } from './features/runtime'
import { Configuration } from './features/configuration'
import { Components } from './features/components'
import { Events } from './features/events'
import { Settings } from './features/settings'

type Page = 'overview'|'runtime'|'configuration'|'components'|'events'|'settings'
export function App(){
  const[page,setPage]=useState<Page>('overview');const[snapshot,setSnapshot]=useState<Snapshot>({system:null,status:null,streams:[],operations:[],events:[]});const[error,setError]=useState('');const[stale,setStale]=useState(false)
  const refresh=useCallback(async()=>{try{const[system,status,streams,operations,events]=await Promise.all([api.system(),api.status(),api.streams(),api.operations(),api.events()]);const nextOperations=(Array.isArray(operations)?operations:operations.items).filter(op=>op.operation);setSnapshot({system:{...system,capabilities:system.capabilities??[]},status,streams:Array.isArray(streams)?streams:streams.items,operations:nextOperations,events:Array.isArray(events)?events:events.items});setStale(false);setError('')}catch(cause){setStale(true);setError(errorMessage(cause))}},[])
  useEffect(()=>{void refresh();const timer=window.setInterval(()=>void refresh(),5000);return()=>window.clearInterval(timer)},[refresh])
  const command=async(id:string,action:'start'|'stop'|'restart')=>{try{const op=await api.command(id,action);await waitForOperation(op.id);await refresh()}catch(cause){setError(errorMessage(cause))}}
  const nav:[Page,string][]=[['overview','Overview'],['runtime','Streams'],['configuration','Configuration'],['components','Components'],['events','Events'],['settings','Settings']]
  return <div className="shell"><aside><h1>arkflow</h1><p>Control plane</p><nav>{nav.map(([key,label])=><a key={key} className={page===key?'active':''} onClick={()=>setPage(key)}>{label}</a>)}</nav></aside><main><header><div><span className="eyebrow">CONTROL PLANE</span><h2>{page}</h2></div><button onClick={()=>void refresh()}>Refresh</button></header>{stale&&<div className="warning">Showing the last known state. Retry when the control API is available.</div>}{error&&<div className="error">{error}</div>}{page==='overview'&&<Overview snapshot={snapshot}/>} {page==='runtime'&&<Runtime streams={snapshot.streams} operations={snapshot.operations} command={command}/>} {page==='configuration'&&<Configuration onError={setError}/>} {page==='components'&&<Components onError={setError}/>} {page==='events'&&<Events events={snapshot.events}/>} {page==='settings'&&<Settings status={snapshot.status}/>}</main></div>
}
async function waitForOperation(id:string){for(let i=0;i<30;i++){const operation=await api.operation(id);if(['succeeded','failed','cancelled','timed_out'].includes(operation.state)){if(operation.state!=='succeeded')throw new Error(operation.error??`Operation ${operation.state}`);return}await new Promise(resolve=>window.setTimeout(resolve,250))}throw new Error('Operation timed out')}
function errorMessage(cause:unknown){return typeof cause==='object'&&cause&&'message'in cause?String(cause.message):cause instanceof Error?cause.message:'Control API unavailable'}
