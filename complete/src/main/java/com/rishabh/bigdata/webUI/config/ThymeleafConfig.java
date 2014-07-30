package com.rishabh.bigdata.webUI.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.thymeleaf.spring4.SpringTemplateEngine;
import org.thymeleaf.spring4.view.ThymeleafViewResolver;
import org.thymeleaf.templateresolver.ServletContextTemplateResolver;
import org.thymeleaf.templateresolver.TemplateResolver;

@Configuration
class ThymeleafConfig {
 
    @Bean
    TemplateResolver templateResolver() {
    	
    	ServletContextTemplateResolver mResolver = new ServletContextTemplateResolver();
    	mResolver.setPrefix("/WEB-INF/templates/");
    	mResolver.setSuffix(".html");
    	mResolver.setTemplateMode("HTML5");
    	
    	mResolver.addTemplateAlias("headerTemplate", "common/header");
    	mResolver.addTemplateAlias("footerTemplate", "common/footer");
    	mResolver.addTemplateAlias("stylesTemplate", "common/styles");
    	mResolver.addTemplateAlias("jsTemplate", "common/js");
    	
        return mResolver;
    }
 
    @Bean
    SpringTemplateEngine templateEngine() {
    	
    	SpringTemplateEngine mTemplateEngine = new SpringTemplateEngine();
    	mTemplateEngine.setTemplateResolver(templateResolver());
    	
        return mTemplateEngine;
    }
 
    @Bean
    ThymeleafViewResolver viewResolver() {
    	
    	ThymeleafViewResolver mViewResolver = new ThymeleafViewResolver();
    	mViewResolver.setTemplateEngine(templateEngine());
    	mViewResolver.setOrder(1);
    	
        return mViewResolver;
    }
}
