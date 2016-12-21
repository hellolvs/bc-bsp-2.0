
#ifndef BSP_PIPES_TEMPLATE_FACTORY_HH
#define BSP_PIPES_TEMPLATE_FACTORY_HH

namespace BSPPipes {

  template <class Vertex, class Edge,class BSP,class AggregatorContainer,class RecordParse>
  class TemplateFactory: public Factory {
  public:
	Vertex* createVertex() const {
		return new Vertex();
	}
	Edge* createEdge() const {
		return new Edge();
	}
	BSP* CreatBsp() const {
		return new BSP();
	}
	AggregatorContainer* CreateAggC() const {
		return new AggregatorContainer();
	}
	RecordParse* creatRecordParse() const{
		return new RecordParse();
	}
  };

  template <class Vertex, class Edge>
   class TemplateFactory1: public Factory {
   public:
 	  Vertex* createVertex() const {
       return new Vertex();
     }
 	  Edge* createEdge() const {
       return new Edge();
     }
   };

}
#endif
