
export const dialogueStorage=()=> {
    const addAutopilotStorage = (dialogue) => {
        sessionStorage.setItem("HoImes_Dialogue_Autopilot", JSON.stringify(dialogue));
    };
    const getAutopilotStorage = () => {
        return JSON.parse(sessionStorage.getItem("HoImes_Dialogue_Autopilot"));
    };
    const addDialogueStorage = (dialogue) => {
        sessionStorage.setItem("HoImes_Dialogue", JSON.stringify(dialogue));
    };
    const getDialogueStorage = () => {
        return JSON.parse(sessionStorage.getItem("HoImes_Dialogue"));
    };
    const addChatList = (dialogue,type) => {
        if(dialogue.length<=0){
            return
        }
        let HoImes_Dialogue=[]
        if(type==="chat"){
            HoImes_Dialogue=getDialogueStorage()
        }else{
            HoImes_Dialogue=getDashboard()
        }
        if(HoImes_Dialogue && HoImes_Dialogue.length>0){
            HoImes_Dialogue[HoImes_Dialogue.length-1].messages=dialogue
        }
        type==="chat"?addDialogueStorage(HoImes_Dialogue):addDashboard(HoImes_Dialogue)
        if(type==="chat"&&HoImes_Dialogue && HoImes_Dialogue.length>0 && HoImes_Dialogue[HoImes_Dialogue.length-1].messages.length>=2){
            addAllStorage(HoImes_Dialogue)
        }
    }
    const addDashboard = (dialogue) => {
        sessionStorage.setItem("HoImes_Dashboard", JSON.stringify(dialogue));
    };
    const setDialogueStorageDashboardId = (id) => {
        let dialogueStorage=getDashboard()
        if(dialogueStorage && dialogueStorage.length>0){
            dialogueStorage[0].dashboardId=id
        }
        addDashboard(dialogueStorage)
    };
    const getDashboard = () => {
        return JSON.parse(sessionStorage.getItem("HoImes_Dashboard"));
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
        localStorage.setItem("HoImes_All", JSON.stringify(allStorage));
    }
    const getAllStorage = () => {
        return JSON.parse(localStorage.getItem("HoImes_All")) || [];
    }
    return {getAutopilotStorage,addAutopilotStorage,addDashboard, getDashboard,addDialogueStorage,getDialogueStorage,addChatList,getAllStorage,setDialogueStorageDashboardId};
}