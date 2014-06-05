package com.rishabh.bigdata.bootstrap;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.web.servlet.support.AbstractAnnotationConfigDispatcherServletInitializer;
import org.springframework.web.WebApplicationInitializer;

import com.rishabh.bigdata.webUI.config.BigDataServletConfig;
import com.rishabh.bigdata.webUI.config.RootConfig;

public class BigDataWebXml extends AbstractAnnotationConfigDispatcherServletInitializer  {
	
	@Override
	protected Class<?>[] getRootConfigClasses() {
		// TODO Auto-generated method stub
				
		return new Class<?>[] {RootConfig.class};
	}

	@Override
	protected Class<?>[] getServletConfigClasses() {

		return new Class<?>[] {BigDataServletConfig.class};
	}

	@Override
	protected String[] getServletMappings() {
		
		return new String[] {"/"};
	}

}
