import React,{useState} from 'react';
import Button from "antd/lib/button";
import next from "@/assets/images/next.png"
import './index.css';
import Link from "@/components/Link";
class Resize extends React.Component {
    constructor(props) {
      super(props);
      this.state = {
        size: false,
      };
    }
   resizeMessage = () => {
    // this.setState({ size:!this.state.size},()=>{
    //   this.props.resize(this.state.size)
    // })
    
  }
  render(){
    return (
        <div className='resize-col' style={{"right":this.state.size?'-5px':'0'}}>
        {/* <div className="resize-btn" onClick={this.resizeMessage}> */}
        
        {/* {
          this.state.size?
          <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="tabler-icon tabler-icon-arrow-bar-right"><path d="M20 12l-10 0"></path><path d="M20 12l-4 4"></path><path d="M20 12l-4 -4"></path><path d="M4 4l0 16"></path></svg>
          :
          <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="tabler-icon tabler-icon-arrow-bar-left"><path d="M4 12l10 0"></path><path d="M4 12l4 4"></path><path d="M4 12l4 -4"></path><path d="M20 4l0 16"></path></svg>
        } */}
        {/* </div> */}
        <Link className="resize-btn" href="/data_sources" target="_blank">
        <i class="zmdi zmdi-settings"></i>
        </Link>
        </div>
      );
  }
};

export default Resize;