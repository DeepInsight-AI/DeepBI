import { useRef, useEffect } from "react";
import location from "@/services/location";

export default function useUnsavedChangesAlert(shouldShowAlert = false) {
  const shouldShowAlertRef = useRef();
  shouldShowAlertRef.current = shouldShowAlert;

  useEffect(() => {
    const unloadMessage = window.W_L.leave_lost_warning;
    const confirmMessage = `${unloadMessage}\n` + window.W_L.leave_lost_confirm ;
    // store original handler (if any)
    const savedOnBeforeUnload = window.onbeforeunload;

    window.onbeforeunload = function onbeforeunload() {
      return shouldShowAlertRef.current ? unloadMessage : undefined;
    };

    const unsubscribe = location.confirmChange((nextLocation, currentLocation) => {
      if (shouldShowAlertRef.current && nextLocation.path !== currentLocation.path) {
        return confirmMessage;
      }
    });

    return () => {
      window.onbeforeunload = savedOnBeforeUnload;
      unsubscribe();
    };
  }, []);
}
