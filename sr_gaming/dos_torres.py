import keras
from keras import Model, layers
from keras.saving import register_keras_serializable

@register_keras_serializable()
class RankingModel(keras.Model):
    def __init__(
        self,
        num_users,
        num_items,
        embedding_dimension=32,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # guardar estos parámetros para get_config
        self.num_users = num_users
        self.num_items = num_items
        self.embedding_dimension = embedding_dimension

        self.user_embedding = keras.layers.Embedding(
            self.num_users, self.embedding_dimension
        )
        self.item_embedding = keras.layers.Embedding(
            self.num_items, self.embedding_dimension
        )

        self.scores = keras.Sequential(
            [
                keras.layers.Dense(256, activation="relu"),
                keras.layers.Dense(64, activation="relu"),
                keras.layers.Dense(1),
            ]
        )

    def call(self, inputs):
        user_id = inputs["user_id"]
        item_id = inputs["item_id"]

        user_embeddings = self.user_embedding(user_id)
        item_embeddings = self.item_embedding(item_id)

        x = keras.ops.concatenate([user_embeddings, item_embeddings], axis=1)
        return self.scores(x)

    def get_config(self):
        # config base de keras.Model (name, trainable, etc.)
        config = super().get_config()
        # agregamos nuestros parámetros del constructor
        config.update(
            {
                "num_users": self.num_users,
                "num_items": self.num_items,
                "embedding_dimension": self.embedding_dimension,
            }
        )
        return config

    @classmethod
    def from_config(cls, config):
        # esto permite reconstruir correctamente desde el config
        return cls(**config)