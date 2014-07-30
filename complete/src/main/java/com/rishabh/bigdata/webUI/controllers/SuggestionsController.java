package com.rishabh.bigdata.webUI.controllers;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class SuggestionsController {
	@RequestMapping(value = "/category/suggestions", method = RequestMethod.GET)
	public String category_form(Model model) {
		return "category/suggestions/index";
	}
	
	@RequestMapping(value="/category/suggestions/view", method=RequestMethod.POST, params="pTitle")
    public String category_view(@RequestParam(value="pTitle", required=false)String mProductTitle, Model model) {
    	StringTokenizer mTokenizer = new StringTokenizer(mProductTitle, " ");
    	
    	List<String> mTokenList = new ArrayList<String>();
    	
    	while (mTokenizer.hasMoreTokens()) {
    		mTokenList.add(mTokenizer.nextToken());
    	}
    	
    	
    	
        return "category/suggestions/view";
    }
}
