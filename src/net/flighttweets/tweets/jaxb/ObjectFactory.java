//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2012.05.04 at 12:32:49 PM EDT 
//


package net.flighttweets.tweets.jaxb;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlElementDecl;
import javax.xml.bind.annotation.XmlRegistry;
import javax.xml.namespace.QName;


/**
 * This object contains factory methods for each 
 * Java content interface and Java element interface 
 * generated in the net.flighttweets.tweets.jaxb package. 
 * <p>An ObjectFactory allows you to programatically 
 * construct new instances of the Java representation 
 * for XML content. The Java representation of XML 
 * content can consist of schema derived interfaces 
 * and classes representing the binding of schema 
 * type definitions, element declarations and model 
 * groups.  Factory methods for each of these are 
 * provided in this class.
 * 
 */
@XmlRegistry
public class ObjectFactory {

    private final static QName _TweetConfig_QNAME = new QName("", "TweetConfig");

    /**
     * Create a new ObjectFactory that can be used to create new instances of schema derived classes for package: net.flighttweets.tweets.jaxb
     * 
     */
    public ObjectFactory() {
    }

    /**
     * Create an instance of {@link EventType }
     * 
     */
    public EventType createEventType() {
        return new EventType();
    }

    /**
     * Create an instance of {@link TweetConfigType }
     * 
     */
    public TweetConfigType createTweetConfigType() {
        return new TweetConfigType();
    }

    /**
     * Create an instance of {@link JAXBElement }{@code <}{@link TweetConfigType }{@code >}}
     * 
     */
    @XmlElementDecl(namespace = "", name = "TweetConfig")
    public JAXBElement<TweetConfigType> createTweetConfig(TweetConfigType value) {
        return new JAXBElement<TweetConfigType>(_TweetConfig_QNAME, TweetConfigType.class, null, value);
    }

}
