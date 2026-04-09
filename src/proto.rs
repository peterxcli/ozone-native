pub mod hadoop {
    pub mod common {
        tonic::include_proto!("hadoop.common");
    }

    pub mod hdds {
        tonic::include_proto!("hadoop.hdds");

        pub mod datanode {
            tonic::include_proto!("hadoop.hdds.datanode");
        }
    }

    pub mod ozone {
        tonic::include_proto!("hadoop.ozone");
    }
}

pub mod ratis {
    pub mod common {
        tonic::include_proto!("ratis.common");
    }

    pub mod grpc {
        tonic::include_proto!("ratis.grpc");
    }
}
