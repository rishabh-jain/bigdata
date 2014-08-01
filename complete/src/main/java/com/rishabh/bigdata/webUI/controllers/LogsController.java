package com.rishabh.bigdata.webUI.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.rishabh.bigdata.log.Logger;

@Controller
public class LogsController {
	@RequestMapping(value = "/logs", method = RequestMethod.GET)
	public String logs_list(Model model) {
		List<String> mLogDirectories = Logger.getInstance().getLogDirectories();
		
		model.addAttribute("logDirectories", mLogDirectories);
		return "logs/index";
	}
	
	@RequestMapping(value="/logs/view", method=RequestMethod.GET, params="dir")
    public String logs_view(@RequestParam(value="dir", required=false)String mLogDir, Model model) {
    	StringBuffer mErrorLogsBuffer = Logger.getInstance().getErrorLogs(mLogDir);
    	StringBuffer mInfoLogsBuffer = Logger.getInstance().getInfoLogs(mLogDir);
    	
    	model.addAttribute("logDir", mLogDir);
    	model.addAttribute("errorLogs", mErrorLogsBuffer);
    	model.addAttribute("infoLogs", mInfoLogsBuffer);
    	
    	return "logs/view";
    }
}
