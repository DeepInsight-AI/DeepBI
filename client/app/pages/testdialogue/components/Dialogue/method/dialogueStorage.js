
export const dialogueStorage=()=> {
    const addAutopilotStorage = (dialogue) => {
        sessionStorage.setItem("Chart_Dialogue_Autopilot", JSON.stringify(dialogue));
    };
    const getAutopilotStorage = () => {
        return JSON.parse(sessionStorage.getItem("Chart_Dialogue_Autopilot"));
    };
    const addDialogueStorage = (dialogue) => {
        sessionStorage.setItem("Chart_Dialogue", JSON.stringify(dialogue));
    };
    const getDialogueStorage = () => {
        return JSON.parse(sessionStorage.getItem("Chart_Dialogue"));
    };
    const addChatList = (dialogue,type) => {
        if(dialogue.length<=0){
            return
        }
        let Chart_Dialogue=[]
        if(type==="chat"){
            Chart_Dialogue=getDialogueStorage()
        }else{
            Chart_Dialogue=getDashboard()
        }
        if(Chart_Dialogue && Chart_Dialogue.length>0){
            Chart_Dialogue[Chart_Dialogue.length-1].messages=dialogue
        }
        type==="chat"?addDialogueStorage(Chart_Dialogue):addDashboard(Chart_Dialogue)
        if(type==="chat"&&Chart_Dialogue && Chart_Dialogue.length>0 && Chart_Dialogue[Chart_Dialogue.length-1].messages.length>=2){
            addAllStorage(Chart_Dialogue)
        }
    }
    const addDashboard = (dialogue) => {
        sessionStorage.setItem("Chart_Dashboard", JSON.stringify(dialogue));
    };
    const setDialogueStorageDashboardId = (id) => {
        let dialogueStorage=getDashboard()
        if(dialogueStorage && dialogueStorage.length>0){
            dialogueStorage[0].dashboardId=id
        }
        addDashboard(dialogueStorage)
    };
    const getDashboard = () => {
        return JSON.parse(sessionStorage.getItem("Chart_Dashboard"));
    };

    const addAllStorage = (dialogue) => {
        let allStorage=getAllStorage()
        if(allStorage.length>=10){
            allStorage.splice(0,1)
        }
        let index=allStorage.findIndex((item)=>{
            return item.uuid===dialogue[0].uuid
        })
        if(index>=0){
            allStorage[index].messages=dialogue[0].messages
        }else{
            allStorage.push(dialogue[0])
        }
        localStorage.setItem("Chart_All", JSON.stringify(allStorage));
    }
    const getAllStorage = () => {
        return JSON.parse(localStorage.getItem("Chart_All")) || [];
    }
    return {getAutopilotStorage,addAutopilotStorage,addDashboard, getDashboard,addDialogueStorage,getDialogueStorage,addChatList,getAllStorage,setDialogueStorageDashboardId};
}