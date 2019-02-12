package com.yahoo.container.jdisc.component;

import com.yahoo.component.ComponentId;
import com.yahoo.container.handler.VipStatus;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.jdisc.JdiscBindingsConfig;
import com.yahoo.container.jdisc.ThreadedHttpRequestHandler;
import com.yahoo.jdisc.handler.RequestHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.Test;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;


public class WarmupTest {

    static class TestHandler extends ThreadedHttpRequestHandler implements RequestHandler {
        public boolean throwException;
        public int nRequests;

        public TestHandler(boolean throwException) {
            super(Executors.newSingleThreadExecutor());
            this.throwException = throwException;
            nRequests = 0;
        }

        @Override
        public HttpResponse handle(HttpRequest request) {
            nRequests++;
            if (throwException)
                throw new RuntimeException("Nah did not work out");

            return new HttpResponse(200) {
                @Override
                public void render(OutputStream outputStream) throws IOException {

                }
            };
        }
    }


    @Test
    public void test_happy_path() {
        VipStatus status = new VipStatus();
        RequestHandler handler = new TestHandler(false);
        Map<ComponentId,RequestHandler>  componentMap = getComponentMap(handler);
        assertThat(status.isInRotation(), is(true));
        new Warmup(status,getConfig("https://*/search/"),componentMap);
        assertThat(status.isInRotation(), is(true));
        assertThat(((TestHandler) handler).nRequests,is(Warmup.REQUESTS));
    }

    @Test
    public void test_exception_during_warmup() {
        VipStatus status = new VipStatus();
        RequestHandler handler = new TestHandler(true);
        Map<ComponentId,RequestHandler>  componentMap = getComponentMap(handler);
        assertThat(status.isInRotation(), is(true));
        new Warmup(status,getConfig("https://*/search/"),componentMap);
        assertThat(status.isInRotation(), is(true));
        assertThat(((TestHandler) handler).nRequests,is(Warmup.REQUESTS));
    }

    @Test
    public void test_no_search_binding() {
        VipStatus status = new VipStatus();
        RequestHandler handler = new TestHandler(false);
        Map<ComponentId,RequestHandler>  componentMap = getComponentMap(handler);
        assertThat(status.isInRotation(), is(true));
        new Warmup(status,getConfig("https://*/somethings/"),componentMap);
        assertThat(status.isInRotation(), is(true));
        assertThat(((TestHandler) handler).nRequests,is(0));
    }



    JdiscBindingsConfig getConfig(String binding) {
        return  new JdiscBindingsConfig(new JdiscBindingsConfig.Builder()
                .handlers("TestHandler", new JdiscBindingsConfig.Handlers.Builder()
                        .serverBindings(binding)
                ));
    }

    Map<ComponentId,RequestHandler> getComponentMap(RequestHandler requestHandler) {
        Map<ComponentId, RequestHandler> componentIdMap = new HashMap<>();
        ComponentId id = new ComponentId("TestHandler");
        componentIdMap.put(id, requestHandler);
        return componentIdMap;
    }
}
